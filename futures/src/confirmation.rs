use futures::{
  TryFuture, Poll,
  task::{Context, Waker},
};
use lapin_async::{
  confirmation::Confirmation,
  wait::NotifyReady,
};

use crate::error::Error;

use std::pin::Pin;

pub struct ConfirmationFuture<T> {
  inner:      Result<Confirmation<T>, Option<Error>>,
  subscribed: bool,
}

struct Watcher(Waker);

impl NotifyReady for Watcher {
  fn notify(&self) {
    self.0.wake_by_ref();
  }
}

impl<T: Unpin> TryFuture for ConfirmationFuture<T> {
  type Ok = T;
  type Error = Error;

  fn try_poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Ok, Self::Error>> {
    let fut = self.get_mut();
    match &mut fut.inner {
      Err(err)         => Poll::Ready(Err(err.take().expect("ConfirmationFuture polled twice but we were in an error state"))),
      Ok(confirmation) => {
        if !fut.subscribed {
          confirmation.subscribe(Box::new(Watcher(cx.waker().clone())));
          fut.subscribed = true;
        }
        confirmation.try_wait().map(Ok).map(Poll::Ready).unwrap_or(Poll::Pending)
      },
    }
  }
}

impl<T> From<Confirmation<T>> for ConfirmationFuture<T> {
  fn from(confirmation: Confirmation<T>) -> Self {
    Self { inner: confirmation.into_result().map_err(Some), subscribed: false }
  }
}
