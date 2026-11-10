"""Default HTML/text templates for the email-otp notification service."""

from __future__ import annotations

EMAIL_OTP_SUBJECT = "Your Supershyft verification code"

EMAIL_OTP_BODY_PLAIN = """Hi {{user.first_name}},

Your one-time password is: {{otp.otp}}

This code is valid for 10 minutes. Do not share it with anyone.

If you did not request this code, please ignore this email or contact support@supershyft.com.

— Support Team, Supershyft"""


def default_email_otp_html() -> str:
    """Branded OTP email with high-contrast digit boxes (use {{otp.digit_boxes}})."""
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Your Supershyft verification code</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f6f5;font-family:Arial,Helvetica,sans-serif;color:#1a1a1a;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#f4f6f5;padding:24px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:520px;background-color:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
          <tr>
            <td style="background:linear-gradient(135deg,#1a4d3e 0%,#256b57 100%);padding:28px 24px;text-align:center;">
              <div style="font-size:26px;font-weight:700;color:#ffffff;letter-spacing:0.5px;">Supershyft</div>
              <div style="font-size:11px;font-weight:600;color:#b8ddd0;letter-spacing:2px;margin-top:6px;">PRECISION HEALTH SOLUTIONS</div>
            </td>
          </tr>
          <tr>
            <td style="padding:32px 28px 8px 28px;">
              <p style="margin:0 0 16px 0;font-size:16px;line-height:1.5;">
                Hi <strong>{{user.first_name}}</strong>,
              </p>
              <p style="margin:0 0 24px 0;font-size:15px;line-height:1.6;color:#333333;">
                You requested a one-time password to access your Supershyft account. Use the code below to complete your verification. This code is valid for <strong>10 minutes</strong>.
              </p>
              <p style="margin:0 0 12px 0;font-size:11px;font-weight:600;color:#8a9a94;text-align:center;letter-spacing:1.5px;">YOUR ONE-TIME PASSWORD</p>
            </td>
          </tr>
          <tr>
            <td style="padding:0 20px 28px 20px;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:linear-gradient(160deg,#1a4d3e 0%,#2a6b55 50%,#1a4d3e 100%);border-radius:14px;">
                <tr>
                  <td style="padding:28px 16px 12px 16px;text-align:center;">
                    {{otp.digit_boxes}}
                  </td>
                </tr>
                <tr>
                  <td style="padding:0 16px 22px 16px;text-align:center;font-size:13px;color:#b8ddd0;">
                    Valid for 10 minutes &bull; Do not share this code
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="padding:0 28px 24px 28px;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#fff8e6;border-left:4px solid #f5b942;border-radius:8px;">
                <tr>
                  <td style="padding:14px 16px;font-size:13px;line-height:1.55;color:#5c4a12;">
                    <strong>Security notice:</strong> Supershyft will never ask for your OTP over call or message. If you did not request this code, please ignore this email or contact our support team immediately.
                  </td>
                </tr>
              </table>
              <p style="margin:20px 0 0 0;font-size:14px;color:#444444;">
                Need help? Reach us at <a href="mailto:support@supershyft.com" style="color:#1a4d3e;font-weight:600;text-decoration:none;">support@supershyft.com</a>
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:20px 28px 28px 28px;border-top:1px solid #eeeeee;text-align:center;">
              <p style="margin:0 0 8px 0;font-size:14px;color:#555555;">Warm regards,<br><strong>Support Team - Supershyft</strong></p>
              <p style="margin:0 0 12px 0;"><a href="https://www.supershyft.com" style="color:#1a4d3e;font-weight:600;text-decoration:none;">www.supershyft.com</a></p>
              <p style="margin:0;font-size:11px;color:#999999;">This is an automated email. Please do not reply directly to this message.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
