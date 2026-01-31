# Telegram Medical Data Warehouse

A comprehensive data pipeline that scrapes medical content from Telegram channels, processes images with YOLO object detection, transforms data with dbt, and provides analytics through a FastAPI interface.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Telegram      │───▶│  PostgreSQL  │───▶│   dbt Models    │
│   Scraper       │    │   Raw Data   │    │ (Staging/Marts) │
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │                       │
┌─────────────────┐           │              ┌─────────────────┐
│  YOLO Object    │───────────┘              │   FastAPI       │
│  Detection      │                          │   Analytics     │
└─────────────────┘                          └─────────────────┘
                              │
                    ┌──────────────────┐
                    │     Dagster      │
                    │  Orchestration   │
                    └──────────────────┘
```

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)
```bash
# Clone and navigate to project
git clone <https://github.com/Yenenesh12/medical-telegram-warehouse.git>
cd telegram-medical-warehouse

# Start all services
docker-compose up
```

**Services will be available at:**
- 🎯 Dagster UI: http://localhost:3000
- 📊 API Documentation: http://localhost:8000/docs
- 🗄️ PostgreSQL: localhost:5432

### Option 2: Manual Setup

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

2. **Environment Configuration**
Create `.env` file:
```env


# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=telegram_warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yene1995
```

3. **Initialize Database**
```bash
python scripts/init_database.py
```

4. **Run Pipeline**
```bash
python scripts/run_pipeline.py
```

## 📋 Components

### 1. Data Ingestion
- **Telegram Scraper** (`src/scraper.py`): Extracts messages and media from Telegram channels
- **Database Loader** (`src/load_to_postgres.py`): Loads raw data into PostgreSQL

### 2. Computer Vision
- **YOLO Detection** (`src/yolo_detect.py`): Object detection on medical images
- **Results Loader** (`src/load_yolo_results.py`): Stores detection results

### 3. Data Transformation
- **dbt Models** (`medical_warehouse/models/`):
  - **Staging**: Clean and standardize raw data
  - **Marts**: Business-ready dimensional models

### 4. Analytics API
- **FastAPI Server** (`api/main.py`): RESTful API for data access
- **Endpoints**:
  - `/api/reports/top-products` - Top mentioned medical products
  - `/api/channels/{name}/activity` - Channel activity trends
  - `/api/search/messages` - Message search
  - `/api/reports/visual-content` - Visual content statistics

### 5. Orchestration
- **Dagster Pipeline** (`pipeline.py`): Automated workflow management
- **Scheduling**: Daily runs at 2 AM
- **Monitoring**: Web UI for pipeline status

## 🛠️ Usage

### Running Individual Components

**Scrape Telegram Data:**
```bash
python scripts/run_scraper.py --days-back 7
```

**Run dbt Transformations:**
```bash
python scripts/run_dbt.py all
```

**Start Analytics API:**
```bash
python scripts/run_api.py
```

**Process Images with YOLO:**
```bash
python src/yolo_detect.py
```

### Pipeline Orchestration

**Interactive Pipeline Runner:**
```bash
python scripts/run_pipeline.py
```

**Dagster Web UI:**
```bash
dagster dev -f pipeline.py -h 0.0.0.0 -p 3000
```

## 📊 Data Models

### Raw Layer (`raw` schema)
- `telegram_messages`: Raw scraped messages
- `image_detections`: YOLO detection results

### Staging Layer (`staging` schema)
- `stg_channels`: Cleaned channel data
- `stg_telegram_messages`: Standardized messages

### Marts Layer (`marts` schema)
- `dim_channels`: Channel dimension
- `fct_messages`: Message facts
- `fct_image_detections`: Detection facts

## 🔧 Configuration

### Database Schema
```sql
-- Schemas created automatically
CREATE SCHEMA raw;      -- Raw ingested data
CREATE SCHEMA staging;  -- Cleaned data
CREATE SCHEMA marts;    -- Business models
CREATE SCHEMA utils;    -- Utility functions
```

### dbt Configuration
- **Project**: `medical_warehouse/dbt_project.yml`
- **Profiles**: `medical_warehouse/profiles.yml`
- **Models**: `medical_warehouse/models/`

### Dagster Configuration
- **Pipeline**: `pipeline.py`
- **Config**: `dagster.yaml`
- **Schedule**: Daily at 2 AM (Africa/Addis_Ababa)

## 📈 Monitoring & Logging

### Dagster UI Features
- Pipeline execution history
- Asset lineage visualization
- Scheduling and sensors
- Error tracking and alerts

### Logging
- Application logs: `logs/`
- Pipeline execution logs in Dagster UI
- API access logs via FastAPI

## 🧪 Testing

### dbt Tests
```bash
# Run all tests
python scripts/run_dbt.py test

# Run specific test
dbt test --select assert_no_future_messages
```

### API Testing
```bash
# Health check
curl http://localhost:8000/

# Get top products
curl http://localhost:8000/api/reports/top-products
```

## 📁 Project Structure

```
telegram-medical-warehouse/
├── api/                    # FastAPI application
│   ├── main.py            # API server
│   ├── database.py        # Database connections
│   └── schemas.py         # Pydantic models
├── medical_warehouse/      # dbt project
│   ├── models/            # Data models
│   │   ├── staging/       # Staging models
│   │   └── marts/         # Business models
│   └── tests/             # dbt tests
├── scripts/               # Utility scripts
│   ├── run_pipeline.py    # Pipeline runner
│   ├── run_api.py         # API server
│   ├── run_scraper.py     # Scraper runner
│   └── init_database.py   # Database setup
├── src/                   # Core modules
│   ├── scraper.py         # Telegram scraper
│   ├── load_to_postgres.py # Data loader
│   ├── yolo_detect.py     # Object detection
│   └── load_yolo_results.py # Results loader
├── pipeline.py            # Dagster pipeline
├── docker-compose.yml     # Docker services
├── Dockerfile            # Container definition
└── requirements.txt      # Python dependencies
```

## 🔒 Security & Privacy

- Telegram API credentials stored in environment variables
- Database connections use environment-based configuration
- No sensitive data committed to version control
- API endpoints can be secured with authentication (extend as needed)

## 🚨 Troubleshooting

### Common Issues

**Database Connection Failed:**
```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check connection
python -c "import psycopg2; psycopg2.connect('postgresql://postgres:yene1995@localhost:5432/telegram_warehouse')"
```

**Telegram API Errors:**
- Verify API credentials in `.env`
- Check phone number format (+1234567890)
- Ensure Telegram account has access to target channels

**dbt Compilation Errors:**
```bash
# Check dbt configuration
dbt debug --profiles-dir medical_warehouse

# Compile models
dbt compile --profiles-dir medical_warehouse
```

**YOLO Detection Issues:**
- Ensure images exist in `data/raw/images/`
- Check YOLO model download (automatic on first run)
- Verify OpenCV installation

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For questions and support:
- Create an issue in the repository
- Check the troubleshooting section above
- Review Dagster logs in the web UI
- Check application logs in the `logs/` directory