"""Print normalized WhatsApp template metadata (admin Look up uses this)."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from modules.notifications.whatsapp_template_proxy import WhatsappTemplateProxy


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("name", nargs="?", default="camp15")
    args = parser.parse_args()
    result = await WhatsappTemplateProxy().lookup(template_name=args.name)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
