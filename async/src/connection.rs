use amq_protocol::frame::{AMQPFrame, GenError, Offset, gen_frame, parse_frame};
use amq_protocol::protocol::{AMQPClass, connection};
use amq_protocol::sasl;
use crossbeam_channel::{self, Sender, Receiver};
use log::{debug, error, trace, warn};

use std::{fmt, result, str};
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};

use crate::channels::Channels;
use crate::configuration::Configuration;
use crate::error;
use crate::types::{AMQPValue, FieldTable};

#[derive(Clone,Debug,PartialEq)]
pub enum ConnectionState {
  Initial,
  Connecting(ConnectingState),
  Connected,
  Closing(ClosingState),
  Closed,
  Error,
}

#[derive(Clone,Debug,PartialEq)]
pub enum ConnectingState {
  Initial,
  SentProtocolHeader(ConnectionProperties),
  ReceivedStart,
  SentStartOk,
  ReceivedTune,
  SentTuneOk,
  SentOpen,
  ReceivedSecure,
  SentSecure,
  ReceivedSecondSecure,
  Error,
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ClosingState {
  Initial,
  SentClose,
  ReceivedClose,
  SentCloseOk,
  ReceivedCloseOk,
  Error,
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ConnectionSASLMechanism {
  PLAIN,
}

impl fmt::Display for ConnectionSASLMechanism {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

#[derive(Clone,Debug,PartialEq)]
pub struct ConnectionProperties {
  pub mechanism:         ConnectionSASLMechanism,
  pub locale:            String,
  pub client_properties: FieldTable,
}

impl Default for ConnectionProperties {
  fn default() -> Self {
    Self {
      mechanism:         ConnectionSASLMechanism::PLAIN,
      locale:            "en_US".to_string(),
      client_properties: FieldTable::new(),
    }
  }
}

#[derive(Clone,Debug,PartialEq)]
pub struct Credentials {
  username: String,
  password: String,
}

impl Default for Credentials {
  fn default() -> Credentials {
    Credentials {
      username: "guest".to_string(),
      password: "guest".to_string(),
    }
  }
}

#[derive(Debug)]
pub struct Connection {
  /// current state of the connection. In normal use it should always be ConnectionState::Connected
  pub state:             ConnectionState,
  pub channels:          Channels,
  pub configuration:     Configuration,
  pub vhost:             String,
  /// list of message to send
      frame_receiver:    Receiver<AMQPFrame>,
  /// We keep a copy so that it never gets shutdown and to create new channels
      frame_sender:      Sender<AMQPFrame>,
  /// credentials are stored in an option to remove them from memory once they are used
      credentials:       Option<Credentials>,
  /// Failed frames we need to try and send back + heartbeats
      // FIXME! rework this?
      priority_frames:   VecDeque<AMQPFrame>,
}

impl Default for Connection {
  fn default() -> Self {
    let (sender, receiver) = crossbeam_channel::unbounded();
    let configuration = Configuration::default();

    Self {
      state:             ConnectionState::Initial,
      channels:          Channels::new(configuration.clone(), sender.clone()),
      configuration,
      vhost:             "/".to_string(),
      frame_receiver:    receiver,
      frame_sender:      sender,
      credentials:       None,
      priority_frames:   VecDeque::new(),
    }
  }
}

impl Connection {
  /// creates a `Connection` object in initial state
  pub fn new() -> Self {
    Default::default()
  }

  pub fn set_credentials(&mut self, username: &str, password: &str) {
    self.credentials = Some(Credentials {
      username: username.to_string(),
      password: password.to_string(),
    });
  }

  pub fn set_vhost(&mut self, vhost: &str) {
    self.vhost = vhost.to_string();
  }

  /// starts the process of connecting to the server
  ///
  /// this will set up the state machine and generates the required messages.
  /// The messages will not be sent until calls to `serialize`
  /// to write the messages to a buffer, or calls to `next_frame`
  /// to obtain the next message to send
  pub fn connect(&mut self, options: ConnectionProperties) -> Result<ConnectionState> {
    if self.state != ConnectionState::Initial {
      self.state = ConnectionState::Error;
      return Err(Error::new(ErrorKind::Other, "invalid state"))
    }

    self.channels.send_frame(0, AMQPFrame::ProtocolHeader).expect("channel 0");
    self.state = ConnectionState::Connecting(ConnectingState::SentProtocolHeader(options));
    Ok(self.state.clone())
  }

  /// next message to send to the network
  ///
  /// returns None if there's no message to send
  pub fn next_frame(&mut self) -> Option<AMQPFrame> {
    self.priority_frames.pop_front().or_else(|| {
      // Error means no message
      self.frame_receiver.try_recv().ok()
    })
  }

  /// writes the next message to a mutable byte slice
  ///
  /// returns how many bytes were written and the current state.
  /// this method can be called repeatedly until the buffer is full or
  /// there are no more frames to send
  pub fn serialize(&mut self, send_buffer: &mut [u8]) -> Result<(usize, ConnectionState)> {
    if let Some(next_msg) = self.next_frame() {
      trace!("will write to buffer: {:?}", next_msg);
      match gen_frame((send_buffer, 0), &next_msg).map(|tup| tup.1) {
        Ok(sz) => {
          Ok((sz, self.state.clone()))
        },
        Err(e) => {
          error!("error generating frame: {:?}", e);
          self.state = ConnectionState::Error;
          match e {
            GenError::BufferTooSmall(_) => {
              // Requeue msg
              self.requeue_frame(next_msg);
              Err(Error::new(ErrorKind::InvalidData, "send buffer too small"))
            },
            GenError::InvalidOffset | GenError::CustomError(_) | GenError::NotYetImplemented => {
              Err(Error::new(ErrorKind::InvalidData, "could not generate"))
            }
          }
        }
      }
    } else {
      Err(Error::new(ErrorKind::WouldBlock, "no new message"))
    }
  }

  /// parses a frame from a byte slice
  ///
  /// returns how many bytes were consumed and the current state.
  ///
  /// This method will update the state machine according to the ReceivedStart
  /// frame with `handle_frame`
  pub fn parse(&mut self, data: &[u8]) -> Result<(usize,ConnectionState)> {
    match parse_frame(data) {
      Ok((i, f)) => {
        let consumed = data.offset(i);

        if let Err(e) = self.handle_frame(f) {
          self.state = ConnectionState::Error;
          Err(Error::new(ErrorKind::Other, format!("failed to handle frame: {:?}", e)))
        } else {
          Ok((consumed, self.state.clone()))
        }
      },
      Err(e) => {
        if e.is_incomplete() {
          Ok((0,self.state.clone()))
        } else {
          self.state = ConnectionState::Error;
          Err(Error::new(ErrorKind::Other, format!("parse error: {:?}", e)))
        }
      }
    }
  }

  /// updates the current state with a new received frame
  pub fn handle_frame(&mut self, f: AMQPFrame) -> result::Result<(), error::Error> {
    trace!("will handle frame: {:?}", f);
    match f {
      AMQPFrame::ProtocolHeader => {
        error!("error: the client should not receive a protocol header");
        self.state = ConnectionState::Error;
      },
      AMQPFrame::Method(channel_id, method) => {
        if channel_id == 0 {
          self.handle_global_method(method);
        } else {
          self.channels.receive_method(channel_id, method)?;
        }
      },
      AMQPFrame::Heartbeat(_) => {
        debug!("received heartbeat from server");
      },
      AMQPFrame::Header(channel_id, _, header) => {
        self.channels.handle_content_header_frame(channel_id, header.body_size, header.properties)?;
      },
      AMQPFrame::Body(channel_id, payload) => {
        self.channels.handle_body_frame(channel_id, payload)?;
      }
    };
    Ok(())
  }

  #[doc(hidden)]
  #[clippy::cyclomatic_complexity = "40"]
  pub fn handle_global_method(&mut self, c: AMQPClass) {
    match self.state.clone() {
      ConnectionState::Initial | ConnectionState::Closed | ConnectionState::Error => {
        error!("Received method in global channel and we're {:?}: {:?}", self.state, c);
        self.state = ConnectionState::Error;
      },
      ConnectionState::Connecting(connecting_state) => {
        match connecting_state {
          ConnectingState::Initial => {
            error!("Received method in global channel and we're Conntecting/Initial: {:?}", c);
            self.state = ConnectionState::Error
          },
          ConnectingState::SentProtocolHeader(mut options) => {
            if let AMQPClass::Connection(connection::AMQPMethod::Start(s)) = c {
              trace!("Server sent Connection::Start: {:?}", s);
              self.state = ConnectionState::Connecting(ConnectingState::ReceivedStart);

              let mechanism = options.mechanism.to_string();
              let locale    = options.locale.clone();

              if !s.mechanisms.split_whitespace().any(|m| m == mechanism) {
                error!("unsupported mechanism: {}", mechanism);
              }
              if !s.locales.split_whitespace().any(|l| l == locale) {
                error!("unsupported locale: {}", mechanism);
              }

              if !options.client_properties.contains_key("product") || !options.client_properties.contains_key("version") {
                options.client_properties.insert("product".to_string(), AMQPValue::LongString(env!("CARGO_PKG_NAME").to_string()));
                options.client_properties.insert("version".to_string(), AMQPValue::LongString(env!("CARGO_PKG_VERSION").to_string()));
              }

              options.client_properties.insert("platform".to_string(), AMQPValue::LongString("rust".to_string()));

              let mut capabilities = FieldTable::new();
              capabilities.insert("publisher_confirms".to_string(), AMQPValue::Boolean(true));
              capabilities.insert("exchange_exchange_bindings".to_string(), AMQPValue::Boolean(true));
              capabilities.insert("basic.nack".to_string(), AMQPValue::Boolean(true));
              capabilities.insert("consumer_cancel_notify".to_string(), AMQPValue::Boolean(true));
              capabilities.insert("connection.blocked".to_string(), AMQPValue::Boolean(true));
              capabilities.insert("authentication_failure_close".to_string(), AMQPValue::Boolean(true));

              options.client_properties.insert("capabilities".to_string(), AMQPValue::FieldTable(capabilities));

              let saved_creds = self.credentials.take().unwrap_or_default();

              let start_ok = AMQPClass::Connection(connection::AMQPMethod::StartOk(
                  connection::StartOk {
                    client_properties: options.client_properties,
                    mechanism,
                    locale,
                    response: sasl::plain_auth_string(&saved_creds.username, &saved_creds.password),
                  }
              ));

              debug!("client sending Connection::StartOk: {:?}", start_ok);
              self.channels.send_method_frame(0, start_ok).expect("channel 0");
              self.state = ConnectionState::Connecting(ConnectingState::SentStartOk);
            } else {
              trace!("waiting for class Connection method Start, got {:?}", c);
              self.state = ConnectionState::Error;
            }
          },
          ConnectingState::ReceivedStart => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
            self.state = ConnectionState::Error
          },
          ConnectingState::SentStartOk => {
            if let AMQPClass::Connection(connection::AMQPMethod::Tune(t)) = c {
              debug!("Server sent Connection::Tune: {:?}", t);
              self.state = ConnectionState::Connecting(ConnectingState::ReceivedTune);

              // If we disable the heartbeat (0) but the server don't, follow him and enable it too
              // If both us and the server want heartbeat enabled, pick the lowest value.
              if self.configuration.heartbeat() == 0 || t.heartbeat != 0 && t.heartbeat < self.configuration.heartbeat() {
                self.configuration.set_heartbeat(t.heartbeat);
              }

              if t.channel_max != 0 {
                // 0 means we want to take the server's value
                // If both us and the server specified a channel_max, pick the lowest value.
                if self.configuration.channel_max() == 0 || t.channel_max < self.configuration.channel_max() {
                  self.configuration.set_channel_max(t.channel_max);
                }
              }
              if self.configuration.channel_max() == 0 {
                self.configuration.set_channel_max(u16::max_value());
              }

              if t.frame_max != 0 {
                // 0 means we want to take the server's value
                // If both us and the server specified a frame_max, pick the lowest value.
                if self.configuration.frame_max() == 0 || t.frame_max < self.configuration.frame_max() {
                  self.configuration.set_frame_max(t.frame_max);
                }
              }
              if self.configuration.frame_max() == 0 {
                self.configuration.set_frame_max(u32::max_value());
              }

              let tune_ok = AMQPClass::Connection(connection::AMQPMethod::TuneOk(
                  connection::TuneOk {
                    channel_max: self.configuration.channel_max(),
                    frame_max:   self.configuration.frame_max(),
                    heartbeat:   self.configuration.heartbeat(),
                  }
              ));

              debug!("client sending Connection::TuneOk: {:?}", tune_ok);

              self.channels.send_method_frame(0, tune_ok).expect("channel 0");
              self.state = ConnectionState::Connecting(ConnectingState::SentTuneOk);

              let open = AMQPClass::Connection(connection::AMQPMethod::Open(connection::Open { virtual_host: self.vhost.clone() }));

              debug!("client sending Connection::Open: {:?}", open);
              self.channels.send_method_frame(0, open).expect("channel 0");
              self.state = ConnectionState::Connecting(ConnectingState::SentOpen);
            } else {
              trace!("waiting for class Connection method Start, got {:?}", c);
              self.state = ConnectionState::Error;
            }
          },
          ConnectingState::ReceivedTune => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
            self.state = ConnectionState::Error
          },
          ConnectingState::SentTuneOk => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
            self.state = ConnectionState::Error
          },
          ConnectingState::SentOpen => {
            trace!("state {:?}\treceived\t{:?}", self.state, c);
            if let AMQPClass::Connection(connection::AMQPMethod::OpenOk(o)) = c {
              debug!("Server sent Connection::OpenOk: {:?}, client now connected", o);
              self.state = ConnectionState::Connected;
            } else {
              trace!("waiting for class Connection method Start, got {:?}", c);
              self.state = ConnectionState::Error;
            }
          },
          ConnectingState::ReceivedSecure => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
            self.state = ConnectionState::Error;
          },
          ConnectingState::SentSecure => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
            self.state = ConnectionState::Error;
          },
          ConnectingState::ReceivedSecondSecure => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
            self.state = ConnectionState::Error;
          },
          ConnectingState::Error => {
            error!("state {:?}\treceived\t{:?}", self.state, c);
          },
        }
      },
      ConnectionState::Connected => {
        warn!("Ignoring method from global channel as we are connected {:?}", c);
      },
      ConnectionState::Closing(_) => {
        warn!("Ignoring method from global channel as we are closing: {:?}", c);
      },
    };
  }

  #[doc(hidden)]
  pub fn send_preemptive_frame(&mut self, frame: AMQPFrame) {
    self.priority_frames.push_front(frame);
  }

  #[doc(hidden)]
  pub fn requeue_frame(&mut self, frame: AMQPFrame) {
    self.priority_frames.push_back(frame);
  }

  #[doc(hidden)]
  pub fn has_pending_frames(&self) -> bool {
    !self.priority_frames.is_empty() || !self.frame_receiver.is_empty()
  }
}

#[cfg(test)]
mod tests {
  use env_logger;

  use super::*;
  use crate::channel::BasicProperties;
  use crate::channel_status::ChannelState;
  use crate::consumer::ConsumerSubscriber;
  use crate::message::Delivery;
  use amq_protocol::protocol::basic;
  use amq_protocol::frame::AMQPContentHeader;

  #[derive(Clone,Debug,PartialEq)]
  struct DummySubscriber;

  impl ConsumerSubscriber for DummySubscriber {
    fn new_delivery(&self, _delivery: Delivery) {}
    fn drop_prefetched_messages(&self) {}
    fn cancel(&self) {}
  }

  #[test]
  fn basic_consume_small_payload() {
    let _ = env_logger::try_init();

    use crate::consumer::Consumer;
    use crate::queue::Queue;

    // Bootstrap connection state to a consuming state
    let mut conn = Connection::new();
    conn.state = ConnectionState::Connected;
    conn.configuration.set_channel_max(2047);
    let channel = conn.channels.create().unwrap();
    channel.status.set_state(ChannelState::Connected);
    let queue_name = "consumed".to_string();
    let mut queue = Queue::new(queue_name.clone(), 0, 0);
    let consumer_tag = "consumer-tag".to_string();
    let consumer = Consumer::new(consumer_tag.clone(), false, false, false, false, Box::new(DummySubscriber));
    queue.consumers.insert(consumer_tag.clone(), consumer);
    conn.channels.get(channel.id()).map(|c| {
      c.queues.register(queue);
    });
    // Now test the state machine behaviour
    {
      let deliver_frame = AMQPFrame::Method(
        channel.id(),
        AMQPClass::Basic(
          basic::AMQPMethod::Deliver(
            basic::Deliver {
              consumer_tag: consumer_tag.clone(),
              delivery_tag: 1,
              redelivered: false,
              exchange: "".to_string(),
              routing_key: queue_name.clone(),
            }
          )
        )
      );
      conn.handle_frame(deliver_frame).unwrap();
      let channel_state = channel.status.state();
      let expected_state = ChannelState::WillReceiveContent(
        queue_name.clone(),
        Some(consumer_tag.clone())
      );
      assert_eq!(channel_state, expected_state);
    }
    {
      let header_frame = AMQPFrame::Header(
        channel.id(),
        60,
        Box::new(AMQPContentHeader {
          class_id: 60,
          weight: 0,
          body_size: 2,
          properties: BasicProperties::default(),
        })
      );
      conn.handle_frame(header_frame).unwrap();
      let channel_state = channel.status.state();
      let expected_state = ChannelState::ReceivingContent(queue_name.clone(), Some(consumer_tag.clone()), 2);
      assert_eq!(channel_state, expected_state);
    }
    {
      let body_frame = AMQPFrame::Body(channel.id(), "{}".as_bytes().to_vec());
      conn.handle_frame(body_frame).unwrap();
      let channel_state = channel.status.state();
      let expected_state = ChannelState::Connected;
      assert_eq!(channel_state, expected_state);
    }
  }

  #[test]
  fn basic_consume_empty_payload() {
    let _ = env_logger::try_init();

    use crate::consumer::Consumer;
    use crate::queue::Queue;

    // Bootstrap connection state to a consuming state
    let mut conn = Connection::new();
    conn.state = ConnectionState::Connected;
    conn.configuration.set_channel_max(2047);
    let channel = conn.channels.create().unwrap();
    channel.status.set_state(ChannelState::Connected);
    let queue_name = "consumed".to_string();
    let mut queue = Queue::new(queue_name.clone(), 0, 0);
    let consumer_tag = "consumer-tag".to_string();
    let consumer = Consumer::new(consumer_tag.clone(), false, false, false, false, Box::new(DummySubscriber));
    queue.consumers.insert(consumer_tag.clone(), consumer);
    conn.channels.get(channel.id()).map(|c| {
      c.queues.register(queue);
    });
    // Now test the state machine behaviour
    {
      let deliver_frame = AMQPFrame::Method(
        channel.id(),
        AMQPClass::Basic(
          basic::AMQPMethod::Deliver(
            basic::Deliver {
              consumer_tag: consumer_tag.clone(),
              delivery_tag: 1,
              redelivered: false,
              exchange: "".to_string(),
              routing_key: queue_name.clone(),
            }
          )
        )
      );
      conn.handle_frame(deliver_frame).unwrap();
      let channel_state = channel.status.state();
      let expected_state = ChannelState::WillReceiveContent(
        queue_name.clone(),
        Some(consumer_tag.clone())
      );
      assert_eq!(channel_state, expected_state);
    }
    {
      let header_frame = AMQPFrame::Header(
        channel.id(),
        60,
        Box::new(AMQPContentHeader {
          class_id: 60,
          weight: 0,
          body_size: 0,
          properties: BasicProperties::default(),
        })
      );
      conn.handle_frame(header_frame).unwrap();
      let channel_state = channel.status.state();
      let expected_state = ChannelState::Connected;
      assert_eq!(channel_state, expected_state);
    }
  }
}
