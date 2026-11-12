//! Reading replies from indexers. An indexer picks its own URL and runs its own server, so a
//! reply can be any length, or never end. Each is read only up to a limit, so that no indexer
//! can make dipper hold an unbounded body in memory.

/// Why a reply's body wasn't read.
#[derive(Debug, thiserror::Error)]
pub(super) enum ReplyError {
    /// The body is longer than the caller allows.
    #[error("the reply is longer than the limit of {limit} bytes")]
    TooLong { limit: usize },
    /// The connection failed or timed out before the body ended.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

/// Read a reply's body, giving up as soon as it is known to be longer than `limit` bytes: at
/// once when its declared length says so, otherwise once more than that many bytes have arrived.
pub(super) async fn read_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, ReplyError> {
    let declared_too_long = response
        .content_length()
        .is_some_and(|declared| usize::try_from(declared).map_or(true, |len| len > limit));
    if declared_too_long {
        return Err(ReplyError::TooLong { limit });
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(ReplyError::TooLong { limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;

    async fn get(url: &str) -> reqwest::Response {
        reqwest::Client::new().get(url).send().await.unwrap()
    }

    #[tokio::test]
    async fn a_body_up_to_the_limit_is_read_whole() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("12345"))
            .mount(&server)
            .await;

        let body = read_body(get(&server.uri()).await, 5).await.unwrap();

        assert_eq!(body, b"12345");
    }

    /// Serve 1 connection: send `head`, then each of `pieces`, then keep the connection open
    /// without sending anything more, as a stalled server would. Returns the server's URL.
    async fn stalling_server(head: &'static str, pieces: Vec<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            for piece in pieces {
                // The client hangs up once it refuses the body.
                if socket.write_all(&piece).await.is_err() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        format!("http://{address}/")
    }

    /// The server declares a long body and sends none of it, so only the declared length can
    /// refuse this reply: a read that waited for the body would never return.
    #[tokio::test]
    async fn a_body_declared_longer_than_the_limit_is_refused_before_any_of_it_arrives() {
        //* Given
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n";
        let response = get(&stalling_server(head, Vec::new()).await).await;

        //* When
        let refused = tokio::time::timeout(Duration::from_secs(5), read_body(response, 5))
            .await
            .expect("the declared length alone must refuse the reply");

        //* Then
        assert!(
            matches!(refused, Err(ReplyError::TooLong { limit: 5 })),
            "{refused:?}"
        );
    }

    /// A server that declares no length is the case a declared length can't catch. This one
    /// sends 4 times the limit and then stalls, so a read that didn't count the bytes arriving
    /// would wait for more until the test's timeout, holding all of it.
    #[tokio::test]
    async fn a_body_of_no_declared_length_is_refused_once_it_passes_the_limit() {
        //* Given a server that streams 256 pieces of 1,024 bytes
        let head = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        let piece = format!("400\r\n{}\r\n", "x".repeat(1024)).into_bytes();
        let response = get(&stalling_server(head, vec![piece; 256]).await).await;

        //* When
        let refused = tokio::time::timeout(Duration::from_secs(5), read_body(response, 64 * 1024))
            .await
            .expect("the read must stop at the limit, not wait for the rest");

        //* Then
        assert!(
            matches!(refused, Err(ReplyError::TooLong { limit: 65_536 })),
            "{refused:?}"
        );
    }
}
