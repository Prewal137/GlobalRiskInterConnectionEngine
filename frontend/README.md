# 🌍 Global Risk Interconnection Platform - Frontend

## 📋 Overview
React-based dashboard for visualizing multi-sector risk assessment with:
- 📊 **Live Dashboard** - Real-time risk monitoring
- 📅 **Historical Analysis** - Temporal risk evolution
- 🗺️ **State Analysis** - Spatial risk distribution
- ⚙️ **What-If Simulation** - Interactive scenario testing

## 🚀 Quick Start

### Prerequisites
- Node.js (v14 or higher)
- Backend server running on `http://localhost:8000`

### Installation

1. **Install dependencies:**
```bash
cd frontend
npm install
```

2. **Start development server:**
```bash
npm start
```

3. **Open browser:**
Navigate to `http://localhost:3000`

## 📁 Project Structure

```
frontend/
├── public/
│   └── index.html                    # HTML entry point
├── src/
│   ├── components/
│   │   ├── Navbar.js                 # Navigation bar
│   │   ├── RiskGraph.js              # NetworkX visualization (Force Graph)
│   │   ├── IndiaMap.js               # Interactive India map
│   │   └── RiskCard.js               # Risk display cards
│   ├── pages/
│   │   ├── LiveDashboard.js          # Page 1: Live monitoring
│   │   ├── HistoricalPage.js         # Page 2: Historical analysis
│   │   ├── StateAnalysis.js          # Page 3: State-level analysis
│   │   └── WhatIfPage.js             # Page 4: What-if simulation
│   ├── services/
│   │   └── api.js                    # Backend API integration
│   ├── App.js                        # Main app component (routing)
│   ├── index.js                      # React entry point
│   └── index.css                     # Global styles
└── package.json                      # Dependencies
```

## 🔗 Backend API Connection

All API calls are in `src/services/api.js`:

```javascript
const API_BASE = 'http://localhost:8000';
```

**Key Endpoints Used:**
- `POST /interconnection/live` - Live risk data
- `GET /interconnection/history/{year}` - Historical data
- `GET /interconnection/state/{state}` - State risk
- `POST /interconnection/what-if` - Simulation

## 🎨 Features

### 1. Live Dashboard
- Risk cards with color-coded status
- Interactive force graph visualization
- India map (state-level risk)
- Before/after cascade comparison table

### 2. Historical Page
- Year selector (2016-2024)
- Timeline animation feature
- Trend charts (Recharts)
- Historical data table

### 3. State Analysis
- Clickable India map
- State-specific impact graph
- Cascade impact metrics
- Detailed state information

### 4. What-If Simulation
- Interactive sliders (0-1) for all sectors
- Real-time cascade simulation
- Visual graph updates
- Before/after comparison

## 🛠️ Tech Stack

- **React 18** - UI framework
- **react-force-graph-2d** - Network visualization
- **react-simple-maps** - Interactive maps
- **recharts** - Charts and graphs
- **styled-components** - CSS-in-JS
- **axios** - HTTP client

## 📊 Graph Visualization

**Nodes:** 7 sectors (Climate, Economy, Trade, Geopolitics, Migration, Social, Infrastructure)

**Node Colors:**
- 🟢 Green: Low risk (< 0.3)
- 🟡 Yellow: Medium risk (0.3 - 0.6)
- 🔴 Red: High risk (> 0.6)

**Edges:**
- Thickness = influence weight
- Bidirectional connections

## 🗺️ Map Features

- Hover → shows risk value
- Click → triggers state analysis
- Color-coded states
- Interactive markers

## 🔧 Configuration

### Backend URL
Change in `src/services/api.js`:
```javascript
const API_BASE = 'http://YOUR_BACKEND_URL:PORT';
```

### Development Mode
Starts on `http://localhost:3000` by default

### Production Build
```bash
npm run build
```

## 🐛 Troubleshooting

### "Cannot connect to backend"
- Ensure backend is running: `cd backend && uvicorn app.main:app --reload`
- Check API URL in `src/services/api.js`
- Check CORS settings in backend

### "Module not found"
```bash
rm -rf node_modules
npm install
```

### Map not showing
- Download India states GeoJSON: Place in `public/india-states.json`
- Or use alternative map library

## 📝 Notes

- India map requires GeoJSON file (`india-states.json`) in `/public`
- All components are responsive
- Loading states included for all API calls
- Error handling implemented

## 🎯 Next Steps

1. Add India states GeoJSON file
2. Deploy to production (Netlify, Vercel, etc.)
3. Add authentication if needed
4. Implement real-time updates (WebSocket)
5. Add more chart types

## 📄 License

MIT

---

**💀🔥 Built for research-grade risk analysis**
