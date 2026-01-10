## Receiving `/external/leave-approvals` webhook

`leave_bp` now posts every approved request to `https://coveralreef.pythonanywhere.com/external/leave-approvals` by default (or to whatever URL you set via `LEAVE_APPROVAL_WEBHOOK_URL`). The payload matches the sample below, so you can process it on another service as soon as the approval happens.

1. **Minimal Flask receiver**  
   ```python
   from flask import Flask, jsonify, request
   import logging

   app = Flask(__name__)
   logging.basicConfig(level=logging.INFO)


   @app.route("/external/leave-approvals", methods=["POST"])
   def handle_leave_approval():
       secret_expected = "your-secret"
       provided = request.headers.get("X-Leave-Webhook-Secret")
       if secret_expected and provided != secret_expected:
           logging.warning("Invalid webhook secret: %s", provided)
           return jsonify({"error": "Invalid webhook secret"}), 403

       payload = request.get_json(silent=True)
       if not payload:
           logging.warning("Missing or invalid JSON payload")
           return jsonify({"error": "JSON body required"}), 400

       logging.info(
           "Leave approval received for %s (%s‑%s) → %s",
           payload.get("email") or payload.get("teacher_name"),
           payload.get("leave_start"),
           payload.get("leave_end"),
           payload.get("status"),
       )

       # TODO: store the payload, trigger notifications, etc.

       return jsonify({
           "status": "ok",
           "teacher": payload.get("email") or payload.get("teacher_name"),
           "date": payload.get("leave_start"),
       })


   if __name__ == "__main__":
       app.run(port=5000, debug=True)
   ```

2. **Test with curl** (adjust host/secret per deployment):
   ```
   curl -X POST https://coveralreef.pythonanywhere.com/external/leave-approvals \
     -H "Content-Type: application/json" \
     -H "X-Leave-Webhook-Secret: your-secret" \
     -d '{
       "request_id":"req-456",
       "email":"jane.doe@charterschools.ae",
       "leave_type":"sick",
       "leave_start":"2026-01-05",
       "leave_end":"2026-01-07",
       "submitted_at":"2026-01-04T14:00:00",
       "status":"approved",
       "reason":"strep throat"
     }'
   ```

3. **Log visibility** – the snippet above logs warnings/infos so you can inspect incoming approvals. Adjust the logging level or integrate with your existing observability stack as needed.

4. **Integrate downstream** – use the decoded JSON to update your tracking system, notify parents/admins, or join it with other datasets. Always respond with HTTP 200/201 for success; any other status will make the primary app log a warning about the webhook failure in `leave_bp` (`leave_bp.py:784-815`).

5. **Match secrets** – ensure the receiving app’s `secret_expected` matches `LEAVE_APPROVAL_WEBHOOK_SECRET` in the sending app. If you don’t set a secret, skip the header and accept `None`.

## Quick test from the Super Admin dashboard

The admin dashboard now includes a "Send a sample leave webhook" card that posts the approved payload to `LEAVE_APPROVAL_WEBHOOK_URL` (falls back to `https://coveralreef.pythonanywhere.com/external/leave-approvals`). Pick a teacher with an email that matches your `schedules.xlsx` data and click **Send test absence** to issue the same contract shown above (including `teacher` details); the response you see there mirrors what the receiver should respond with, and retries only happen on non-2xx status codes.
