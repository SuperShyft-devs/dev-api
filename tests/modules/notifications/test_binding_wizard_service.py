import pytest

from modules.notifications.binding_wizard_service import (
    build_template_variables,
    derive_service_key,
)
from modules.notifications.binding_schemas import WizardTemplateHeader, WizardTemplateSlot


def test_derive_service_key():
    assert derive_service_key("auth.otp_sent", "whatsapp") == "auth-otp-sent-whatsapp"


def test_build_template_variables_from_slots():
    tv = build_template_variables(
        template_variables=None,
        template_slots=[
            WizardTemplateSlot(index=1, source="literal", value="Hi"),
            WizardTemplateSlot(index=2, source="user.first_name"),
        ],
    )
    assert tv == {
        "slots": [
            {"index": 1, "source": "literal", "value": "Hi"},
            {"index": 2, "source": "user.first_name"},
        ]
    }


def test_build_template_variables_with_header():
    tv = build_template_variables(
        template_variables=None,
        template_slots=[WizardTemplateSlot(index=1, source="user.first_name")],
        template_header=WizardTemplateHeader(
            source="literal",
            value="https://cdn.example.com/pretest.pdf",
        ),
    )
    assert tv == {
        "slots": [{"index": 1, "source": "user.first_name"}],
        "header": {
            "source": "literal",
            "value": "https://cdn.example.com/pretest.pdf",
        },
    }
