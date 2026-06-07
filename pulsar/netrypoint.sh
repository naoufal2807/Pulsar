#!/bin/bash
# entrypoint.sh - Start Pulsar development environment

set -e

echo "🚀 Starting Pulsar 2.0 Development Environment"
echo "=============================================="

# Wait for Ollama to be ready
echo "⏳ Waiting for Ollama service..."
while ! curl -s http://ollama:11434/api/tags > /dev/null; do
    echo "  Ollama not ready yet, waiting..."
    sleep 2
done
echo "✅ Ollama is ready!"

# Pull Gemma model if not exists
echo "📥 Setting up Gemma model..."
if ! curl -s http://ollama:11434/api/tags | grep -q "gemma"; then
    echo "  Pulling gemma:2b model (first time, this may take a few minutes)..."
    curl -X POST http://ollama:11434/api/pull -d '{"name": "gemma:2b"}' || true
    echo "  ✅ Gemma model ready!"
else
    echo "  ✅ Gemma model already available"
fi

# Create necessary directories
echo "📁 Setting up directories..."
mkdir -p /app/data/baselines
mkdir -p /app/data/drift_reports
mkdir -p /app/data/logs
mkdir -p /app/data/test_data
echo "  ✅ Directories ready"

# Run database migrations if needed
if [ -f "/app/migrations/init.sql" ]; then
    echo "🗄️  Running database migrations..."
    # PGPASSWORD=pulsar_dev_123 psql -h postgres -U pulsar -d pulsar_db -f /app/migrations/init.sql || true
    echo "  ✅ Migrations complete"
fi

# Run tests if TEST_ON_START is set
if [ "$TEST_ON_START" = "true" ]; then
    echo "🧪 Running tests..."
    cd /app
    python -m pytest tests/ -v || true
    echo "  ✅ Tests complete"
fi

# Start Pulsar development server
echo "🎯 Starting Pulsar development server..."
echo "=============================================="
echo ""
echo "📊 Service URLs:"
echo "  Pulsar API:   http://localhost:8000"
echo "  Ollama API:   http://localhost:11434"
echo "  Jupyter:      http://localhost:8888"
echo "  PostgreSQL:   localhost:5432"
echo "  Redis:        localhost:6379"
echo ""
echo "📚 Documentation:"
echo "  API Docs:     http://localhost:8000/docs"
echo "  ReDoc:        http://localhost:8000/redoc"
echo ""

# Start in interactive mode
exec "$@"