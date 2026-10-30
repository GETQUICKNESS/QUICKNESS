import { connect } from "cloudflare:sockets";

/*
 QUICKNESS
 */

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket", { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    let tcp = null;
    let writer = null;

    server.addEventListener("message", async (event) => {
      let msg;
      try { msg = JSON.parse(event.data); }
      catch { server.close(1008, "not json"); return; }

      const { SNI, connect_port, packet } = msg;
      if (!SNI || !connect_port || !packet) {
        server.close(1008, "bad message");
        return;
      }

      let bytes;
      try {
        bytes = Uint8Array.from(atob(packet), c => c.charCodeAt(0));
      } catch {
        server.close(1008, "bad base64");
        return;
      }

      try {
        if (tcp === null) {
          tcp = connect({ hostname: SNI, port: connect_port });
          writer = tcp.writable.getWriter();

          (async () => {
            const reader = tcp.readable.getReader();
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                server.send(value);
              }
            } catch {}
            try { server.close(); } catch {}
          })();
        }
        await writer.write(bytes);
      } catch {
        try { server.close(1011, "tcp failed"); } catch {}
      }
    });

    server.addEventListener("close", () => {
      try { tcp?.close(); } catch {}
    });

    return new Response(null, { status: 101, webSocket: client });
  },
};
