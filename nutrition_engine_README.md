# Nutrition Intelligence Engine — backend integration

See `modules/reports/nutrition_intelligence/README.md` for the full guide.

Quick start:

```python
from nutrition_engine import calculate_nutrition

result = calculate_nutrition(payload)
```

```bash
uvicorn nutrition_main:app --host 0.0.0.0 --port 8000
```

```
POST http://localhost:8000/calculate
Content-Type: application/json
```
