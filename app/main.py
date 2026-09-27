from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import call

app = FastAPI(
    title="WebRTC Signaling Service",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(call.router)


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "Signaling Engine"}