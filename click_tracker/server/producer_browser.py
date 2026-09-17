#!/usr/bin/env python3

import json
import socketserver
from http.server import HTTPServer, BaseHTTPRequestHandler
from confluent_kafka import Producer


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "chrome-clicks"


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


# ============================================================
# Kafka delivery callback
# ============================================================

def delivery_report(err, msg):
    if err is not None:
        print(
            f"Kafka delivery failed: {err}",
            flush=True
        )
    else:
        print(
            f"Kafka message delivered to "
            f"{msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}",
            flush=True
        )


# ============================================================
# HTTP server
# ============================================================

class ThreadingHTTPServer(
    socketserver.ThreadingMixIn,
    HTTPServer
):
    daemon_threads = True


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    # --------------------------------------------------------
    # CORS headers
    # --------------------------------------------------------

    def _send_cors_headers(self):
        self.send_header(
            "Access-Control-Allow-Origin",
            "*"
        )

        self.send_header(
            "Access-Control-Allow-Methods",
            "POST, OPTIONS"
        )

        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type"
        )

    # --------------------------------------------------------
    # Handle browser CORS preflight
    # --------------------------------------------------------

    def do_OPTIONS(self):

        self.send_response(204)

        self._send_cors_headers()

        self.end_headers()

    # --------------------------------------------------------
    # Handle incoming Chrome click event
    # --------------------------------------------------------

    def do_POST(self):

        try:

            # Read request body
            content_length = int(
                self.headers.get(
                    "Content-Length",
                    0
                )
            )

            post_data = self.rfile.read(
                content_length
            )

            # Convert JSON to Python dictionary
            data = json.loads(
                post_data.decode("utf-8")
            )

            # ------------------------------------------------
            # Extract only the fields we want
            # ------------------------------------------------

            result = {
                "x": data.get("x"),
                "y": data.get("y"),
                "pageName": data.get("pageName"),
                "timestamp": data.get("timestamp")
            }

            # ------------------------------------------------
            # Print received event
            # ------------------------------------------------

            print(
                f"json results: {result}",
                flush=True
            )

            # ------------------------------------------------
            # Convert dictionary to JSON
            # ------------------------------------------------

            message = json.dumps(result)

            # ------------------------------------------------
            # Send message to Kafka
            # ------------------------------------------------

            producer.produce(
                topic=KAFKA_TOPIC,
                value=message.encode("utf-8"),
                callback=delivery_report
            )

            # Process Kafka events
            producer.poll(0)

            # ------------------------------------------------
            # Send HTTP response back to Chrome
            # ------------------------------------------------

            self.send_response(200)

            self._send_cors_headers()

            self.send_header(
                "Content-Type",
                "application/json"
            )

            self.end_headers()

            response = {
                "status": "ok"
            }

            self.wfile.write(
                json.dumps(response).encode("utf-8")
            )

        except Exception as e:

            print(
                f"Error: {e}",
                flush=True
            )

            self.send_response(400)

            self._send_cors_headers()

            self.send_header(
                "Content-Type",
                "application/json"
            )

            self.end_headers()

            response = {
                "status": "error",
                "message": str(e)
            }

            self.wfile.write(
                json.dumps(response).encode("utf-8")
            )

    # --------------------------------------------------------
    # Suppress default HTTP logs
    # --------------------------------------------------------

    def log_message(self, format, *args):
        return


# ============================================================
# Start application
# ============================================================

print(
    f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS}",
    flush=True
)

print(
    f"Kafka topic: {KAFKA_TOPIC}",
    flush=True
)

print(
    "Listening on http://localhost:3000/click ...",
    flush=True
)


server = ThreadingHTTPServer(
    ("localhost", 3000),
    SimpleHTTPRequestHandler
)


try:

    server.serve_forever()

except KeyboardInterrupt:

    print(
        "\nStopping server...",
        flush=True
    )

finally:

    # Wait for pending Kafka messages
    producer.flush()

    # Close HTTP server
    server.server_close()
