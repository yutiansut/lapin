use futures::{
  TryFuture, Poll,
  task::Context,
};
use lapin_async::{
  Connect as LapinAsyncConnect,
  confirmation::Confirmation,
  connection::Connection,
  credentials::Credentials,
  uri::AMQPUri,
};

use crate::{
  channel::Channel,
  confirmation::ConfirmationFuture,
  error::Error,
};

pub use lapin_async::connection_properties::{ConnectionSASLMechanism, ConnectionProperties};

use std::pin::Pin;

/// Connect to a server and create channels
#[derive(Clone)]
pub struct Client {
  conn: Connection,
}

impl Client {
  /// Connect to an AMQP Server
  pub fn connect(uri: &str, credentials: Credentials, options: ConnectionProperties) -> ClientFuture {
    Connect::connect(uri, credentials, options)
  }

  /// Connect to an AMQP Server
  pub fn connect_uri(uri: AMQPUri, credentials: Credentials, options: ConnectionProperties) -> ClientFuture {
    Connect::connect(uri, credentials, options)
  }

  /// Return a future that resolves to a `Channel` once the method succeeds
  pub fn create_channel(&self) -> impl TryFuture<Ok = Channel, Error = Error> + Send + 'static {
    Channel::create(self.conn.clone())
  }
}

pub struct ClientFuture(ConfirmationFuture<Connection>);

impl TryFuture for ClientFuture {
  type Ok = Client;
  type Error = Error;

  fn try_poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Ok, Self::Error>> {
    Pin::new(&mut self.get_mut().0).try_poll(cx).map_ok(|conn| Client { conn })
  }
}

impl From<Confirmation<Connection>> for ClientFuture {
  fn from(confirmation: Confirmation<Connection>) -> Self {
    Self(confirmation.into())
  }
}

/// Trait providing a method to connect to an AMQP server
pub trait Connect {
  /// Connect to an AMQP server
  fn connect(self, credentials: Credentials, options: ConnectionProperties) -> ClientFuture;
}

impl Connect for AMQPUri {
  fn connect(self, credentials: Credentials, options: ConnectionProperties) -> ClientFuture {
    LapinAsyncConnect::connect(self, credentials, options).into()
  }
}

impl Connect for &str {
  fn connect(self, credentials: Credentials, options: ConnectionProperties) -> ClientFuture {
    LapinAsyncConnect::connect(self, credentials, options).into()
  }
}
