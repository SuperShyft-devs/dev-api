from modules.notifications.whatsapp_template_proxy import normalize_template_response


def test_normalize_template_response_counts_body_slots():
    raw = {
        "data": [
            {
                "name": "camp17",
                "language": "en",
                "status": "APPROVED",
                "category": "UTILITY",
                "parameter_format": "POSITIONAL",
                "components": [
                    {
                        "type": "BODY",
                        "text": "Hello {{1}}, {{2}}",
                    }
                ],
            }
        ]
    }
    out = normalize_template_response(raw)
    assert out is not None
    assert out["body_positional_count"] == 2
    assert out["status"] == "APPROVED"


def test_normalize_template_response_header_document():
    raw = {
        "data": [
            {
                "name": "camp15",
                "status": "APPROVED",
                "components": [
                    {"type": "HEADER", "format": "DOCUMENT"},
                    {"type": "BODY", "text": "Hello {{1}}, {{2}}"},
                ],
            }
        ]
    }
    out = normalize_template_response(raw)
    assert out is not None
    assert out["header_format"] == "DOCUMENT"
    assert out["body_positional_count"] == 2
