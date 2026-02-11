"""
CryptoPulse - REST API

FastAPI application for serving predictions, features, and anomaly alerts.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from cryptopulse import __version__
from cryptopulse.config import settings
from cryptopulse.logging import setup_logging, get_api_logger
from cryptopulse.models import (
    HealthStatus,
    PredictionResponse,
    PricePrediction,
    MarketFeatures,
    AggregatedSentiment,
    AnomalyAlert,
    AnomalyListResponse,
    PriceDirection,
    SentimentLabel,
    AnomalyType,
)

from api.services import (
    PredictionService,
    FeatureService,
    AnomalyService,
)

logger = get_api_logger()


# =============================================================================
# Application Lifespan
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    setup_logging()
    logger.info("starting_cryptopulse_api", version=__version__)
    
    # Initialize services
    app.state.prediction_service = PredictionService()
    app.state.feature_service = FeatureService()
    app.state.anomaly_service = AnomalyService()
    
    await app.state.prediction_service.initialize()
    await app.state.feature_service.initialize()
    await app.state.anomaly_service.initialize()

    # Initialize LangGraph agent orchestrator
    from agents.orchestrator import AgentOrchestrator
    app.state.agent_orchestrator = AgentOrchestrator()
    await app.state.agent_orchestrator.initialize()
    
    logger.info("services_initialized")
    
    yield
    
    # Shutdown
    logger.info("shutting_down_api")


# =============================================================================
# App Configuration
# =============================================================================

app = FastAPI(
    title="CryptoPulse API",
    description="Real-time cryptocurrency market intelligence and prediction API",
    version=__version__,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Dependency Injection
# =============================================================================

def get_prediction_service() -> PredictionService:
    return app.state.prediction_service


def get_feature_service() -> FeatureService:
    return app.state.feature_service


def get_anomaly_service() -> AnomalyService:
    return app.state.anomaly_service


# =============================================================================
# Health Endpoints
# =============================================================================

@app.get("/", tags=["Health"])
async def root():
    """API root endpoint."""
    return {"name": "CryptoPulse API", "version": __version__}


@app.get("/health", response_model=HealthStatus, tags=["Health"])
async def health_check(
    prediction_svc: PredictionService = Depends(get_prediction_service),
    feature_svc: FeatureService = Depends(get_feature_service),
    anomaly_svc: AnomalyService = Depends(get_anomaly_service),
):
    """
    Check API health and service status.
    """
    return HealthStatus(
        status="healthy",
        version=__version__,
        timestamp=datetime.utcnow(),
        services={
            "prediction": prediction_svc.is_ready,
            "features": feature_svc.is_ready,
            "anomaly": anomaly_svc.is_ready,
        },
    )


# =============================================================================
# Prediction Endpoints
# =============================================================================

@app.get(
    "/api/v1/predictions/{symbol}",
    response_model=PredictionResponse,
    tags=["Predictions"],
)
async def get_prediction(
    symbol: str,
    horizon_minutes: int = Query(default=5, ge=1, le=15),
    include_features: bool = Query(default=False),
    include_sentiment: bool = Query(default=False),
    prediction_svc: PredictionService = Depends(get_prediction_service),
    feature_svc: FeatureService = Depends(get_feature_service),
):
    """
    Get price direction prediction for a trading pair.
    
    - **symbol**: Trading pair (e.g., BTCUSDT)
    - **horizon_minutes**: Prediction horizon (1-15 minutes)
    - **include_features**: Include current market features
    - **include_sentiment**: Include current sentiment score
    """
    symbol = symbol.upper()
    
    try:
        prediction = await prediction_svc.get_prediction(symbol, horizon_minutes)
        
        if prediction is None:
            raise HTTPException(
                status_code=404,
                detail=f"No prediction available for {symbol}",
            )
        
        response = PredictionResponse(
            symbol=symbol,
            prediction=prediction,
        )
        
        if include_features:
            response.features = await feature_svc.get_latest_features(symbol)
        
        if include_sentiment:
            response.sentiment = await feature_svc.get_latest_sentiment(symbol)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("prediction_error", symbol=symbol, error=str(e))
        raise HTTPException(status_code=500, detail="Prediction service error")


@app.get(
    "/api/v1/predictions",
    tags=["Predictions"],
)
async def get_all_predictions(
    symbols: str = Query(
        default="BTCUSDT,ETHUSDT,BNBUSDT",
        description="Comma-separated list of symbols",
    ),
    horizon_minutes: int = Query(default=5, ge=1, le=15),
    prediction_svc: PredictionService = Depends(get_prediction_service),
):
    """
    Get predictions for multiple trading pairs.
    """
    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    
    predictions = {}
    for symbol in symbol_list[:20]:  # Limit to 20 symbols
        pred = await prediction_svc.get_prediction(symbol, horizon_minutes)
        if pred:
            predictions[symbol] = pred.model_dump()
    
    return {
        "count": len(predictions),
        "horizon_minutes": horizon_minutes,
        "predictions": predictions,
    }


# =============================================================================
# Feature Endpoints
# =============================================================================

@app.get(
    "/api/v1/features/{symbol}",
    response_model=MarketFeatures,
    tags=["Features"],
)
async def get_features(
    symbol: str,
    interval: str = Query(default="1m", pattern="^(1m|5m|15m)$"),
    feature_svc: FeatureService = Depends(get_feature_service),
):
    """
    Get current market features for a trading pair.
    
    - **symbol**: Trading pair (e.g., BTCUSDT)
    - **interval**: Feature interval (1m, 5m, 15m)
    """
    symbol = symbol.upper()
    
    features = await feature_svc.get_latest_features(symbol, interval)
    
    if features is None:
        raise HTTPException(
            status_code=404,
            detail=f"No features available for {symbol}",
        )
    
    return features


@app.get(
    "/api/v1/features/{symbol}/history",
    tags=["Features"],
)
async def get_feature_history(
    symbol: str,
    interval: str = Query(default="1m", pattern="^(1m|5m|15m)$"),
    limit: int = Query(default=60, ge=1, le=1000),
    feature_svc: FeatureService = Depends(get_feature_service),
):
    """
    Get historical market features for a trading pair.
    """
    symbol = symbol.upper()
    
    history = await feature_svc.get_feature_history(symbol, interval, limit)
    
    return {
        "symbol": symbol,
        "interval": interval,
        "count": len(history),
        "data": history,
    }


# =============================================================================
# Anomaly Endpoints
# =============================================================================

@app.get(
    "/api/v1/anomalies",
    response_model=AnomalyListResponse,
    tags=["Anomalies"],
)
async def get_anomalies(
    symbol: str | None = Query(default=None, description="Filter by symbol"),
    severity_min: float = Query(default=0.5, ge=0, le=1),
    minutes_back: int = Query(default=60, ge=1, le=1440),
    limit: int = Query(default=50, ge=1, le=200),
    anomaly_svc: AnomalyService = Depends(get_anomaly_service),
):
    """
    Get recent anomaly alerts.
    
    - **symbol**: Optional filter by trading pair
    - **severity_min**: Minimum severity threshold (0-1)
    - **minutes_back**: Look back period in minutes
    - **limit**: Maximum number of alerts
    """
    from_time = datetime.utcnow() - timedelta(minutes=minutes_back)
    to_time = datetime.utcnow()
    
    anomalies = await anomaly_svc.get_anomalies(
        symbol=symbol.upper() if symbol else None,
        from_time=from_time,
        to_time=to_time,
        severity_min=severity_min,
        limit=limit,
    )
    
    return AnomalyListResponse(
        count=len(anomalies),
        anomalies=anomalies,
        from_timestamp=from_time,
        to_timestamp=to_time,
    )


@app.get(
    "/api/v1/anomalies/{symbol}",
    tags=["Anomalies"],
)
async def get_symbol_anomalies(
    symbol: str,
    minutes_back: int = Query(default=60, ge=1, le=1440),
    anomaly_svc: AnomalyService = Depends(get_anomaly_service),
):
    """
    Get anomaly alerts for a specific trading pair.
    """
    symbol = symbol.upper()
    from_time = datetime.utcnow() - timedelta(minutes=minutes_back)
    
    anomalies = await anomaly_svc.get_anomalies(
        symbol=symbol,
        from_time=from_time,
        to_time=datetime.utcnow(),
        limit=50,
    )
    
    return {
        "symbol": symbol,
        "count": len(anomalies),
        "anomalies": [a.model_dump() for a in anomalies],
    }


# =============================================================================
# Market Overview Endpoints
# =============================================================================

@app.get("/api/v1/market/overview", tags=["Market"])
async def get_market_overview(
    feature_svc: FeatureService = Depends(get_feature_service),
    prediction_svc: PredictionService = Depends(get_prediction_service),
    anomaly_svc: AnomalyService = Depends(get_anomaly_service),
):
    """
    Get overall market overview with top movers and alerts.
    """
    # Get top trading pairs
    top_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    
    overview = {
        "timestamp": datetime.utcnow().isoformat(),
        "pairs": {},
        "recent_anomalies": 0,
        "bullish_count": 0,
        "bearish_count": 0,
    }
    
    for symbol in top_symbols:
        features = await feature_svc.get_latest_features(symbol)
        prediction = await prediction_svc.get_prediction(symbol, 5)
        
        if features:
            overview["pairs"][symbol] = {
                "price": float(features.close) if features else None,
                "change_pct": features.price_change_pct if features else None,
                "volume": float(features.volume) if features else None,
                "prediction": prediction.direction if prediction else None,
                "confidence": prediction.confidence if prediction else None,
            }
            
            if prediction:
                if prediction.direction == PriceDirection.UP:
                    overview["bullish_count"] += 1
                elif prediction.direction == PriceDirection.DOWN:
                    overview["bearish_count"] += 1
    
    # Count recent anomalies
    recent = await anomaly_svc.get_anomalies(
        from_time=datetime.utcnow() - timedelta(hours=1),
        to_time=datetime.utcnow(),
        limit=100,
    )
    overview["recent_anomalies"] = len(recent)
    
    return overview


# =============================================================================
# LangGraph Agent Analysis Endpoint
# =============================================================================

@app.post("/analyze/{symbol}", tags=["analysis"])
async def analyze_symbol(
    symbol: str,
    horizon_minutes: int = Query(default=5, ge=1, le=60),
):
    """
    Run a full multi-agent LangGraph analysis for a trading pair.

    The analysis pipeline runs 4 specialist agents:
    - **MarketAnalyst**: Technical indicator analysis
    - **SentimentAnalyst**: News sentiment reasoning
    - **RiskManager**: Anomaly assessment & risk scoring
    - **PortfolioStrategist**: Signal fusion & trading recommendations

    Returns a comprehensive recommendation with confidence score.
    """
    try:
        orchestrator = app.state.agent_orchestrator
        result = await orchestrator.analyze(
            symbol=symbol.upper(),
            horizon_minutes=horizon_minutes,
        )

        return {
            "symbol": result.symbol,
            "recommendation": result.recommendation,
            "confidence": result.confidence,
            "prediction": result.prediction,
            "agent_trace": result.agent_trace,
            "latency_ms": result.total_latency_ms,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error("analysis_failed", symbol=symbol, error=str(e))
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


# =============================================================================
# Error Handlers
# =============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "api.main:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=settings.api.reload,
        workers=1 if settings.api.reload else settings.api.workers,
    )
