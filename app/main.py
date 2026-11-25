from fastapi import FastAPI

from app.api.payments import router as payments_router

app = FastAPI(title="Payments Service", version="0.1.0")
app.include_router(payments_router)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
