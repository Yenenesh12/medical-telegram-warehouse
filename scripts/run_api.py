#!/usr/bin/env python3
"""
Run the FastAPI application
"""

import subprocess
import sys
from pathlib import Path
import webbrowser
import time

def check_dependencies():
    """Check if required packages are installed"""
    required = ['fastapi', 'uvicorn', 'psycopg2']
    missing = []
    
    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    
    return missing

def main():
    """Main function to run the API"""
    print("=" * 60)
    print("Telegram Medical Data Warehouse - API Server")
    print("=" * 60)
    
    # Check dependencies
    missing = check_dependencies()
    if missing:
        print(f"\n✗ Missing dependencies: {', '.join(missing)}")
        print("\nPlease install them using:")
        print("pip install fastapi uvicorn[standard] psycopg2-binary")
        return 1
    
    print("\n✓ All dependencies satisfied")
    print("\nStarting API server...")
    
    # Import and run the API
    try:
        from api.main import start_server
        
        # Open browser to docs
        def open_browser():
            time.sleep(2)
            webbrowser.open("http://localhost:8000/docs")
        
        import threading
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
        
        print("\n" + "=" * 60)
        print("API SERVER STARTED")
        print("=" * 60)
        print("\nAPI Endpoints:")
        print("  • http://localhost:8000/ - Health check")
        print("  • http://localhost:8000/docs - Interactive API documentation")
        print("  • http://localhost:8000/redoc - Alternative documentation")
        print("\nAvailable Endpoints:")
        print("  • GET /api/reports/top-products - Top mentioned medical products")
        print("  • GET /api/channels/{name}/activity - Channel activity trends")
        print("  • GET /api/search/messages - Search messages by keyword")
        print("  • GET /api/reports/visual-content - Visual content statistics")
        print("  • GET /api/analytics/summary - Comprehensive analytics")
        print("\nPress Ctrl+C to stop the server")
        print("=" * 60)
        
        # Start server
        start_server()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nAPI server stopped by user")
        return 0
    except Exception as e:
        print(f"\n✗ Error starting API server: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())