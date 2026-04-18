import axios from 'axios';



const api = axios.create({
  baseURL: 'http://localhost:8000',
  timeout: 60000, // reduce timeout (2 min → 60 sec)
  headers: {
    'Content-Type': 'application/json',
  },
});


// ============================================================
// LIVE DASHBOARD APIs
// ============================================================

export const getLiveRisk = async () => {
  try {
    const response = await api.get('/live/risk');
    return response.data;
  } catch (error) {
    console.error('Live Risk API Error:', error);
    throw error;
  }
};

// OPTIONAL: faster (no ML, just raw data)
export const getLiveData = async () => {
  const response = await api.get('/live/');
  return response.data;
};
// ============================================================
// HISTORICAL APIs
// ============================================================

export const getHistoricalRisk = async (year) => {
  const response = await api.get(`/interconnection/history/${year}`);
  return response.data;
};

export const getTrendData = async (country = 'IND') => {
  const response = await api.get(`/interconnection/trend/${country}`);
  return response.data;
};

export const getHighRiskMonths = async (country = 'IND') => {
  const response = await api.get(`/interconnection/high-risk/${country}`);
  return response.data;
};

// ============================================================
// STATE ANALYSIS APIs
// ============================================================

export const getStateRisk = async (state) => {
  const response = await api.get(`/interconnection/state/${state}`);
  return response.data;
};

export const getStateImpact = async (state) => {
  const response = await api.get(`/interconnection/state-impact/${state}`);
  return response.data;
};

// ============================================================
// WHAT-IF SIMULATION APIs
// ============================================================

export const runWhatIfSimulation = async (payload) => {
  const response = await api.post('/interconnection/what-if', payload);
  return response.data;
};

export const runCustomSimulation = async (payload) => {
  const response = await api.post('/interconnection/custom', payload);
  return response.data;
};

export const simulateShock = async (sector, value) => {
  const response = await api.get(`/interconnection/shock/${sector}/${value}`);
  return response.data;
};

// ============================================================
// DYNAMIC GRAPH APIs
// ============================================================

export const getDynamicRisk = async () => {
  const response = await api.get('/interconnection/dynamic');
  return response.data;
};

export const compareStaticDynamic = async () => {
  const response = await api.get('/interconnection/compare');
  return response.data;
};

export default api;

