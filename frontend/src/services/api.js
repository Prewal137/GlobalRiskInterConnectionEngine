import axios from 'axios';

const API_BASE = 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE,
  timeout: 120000, // 2 minutes for heavy computations
  headers: {
    'Content-Type': 'application/json',
  },
});

// ============================================================
// LIVE DASHBOARD APIs
// ============================================================

export const getLiveRisk = async () => {
  const response = await api.post('/interconnection/live');
  return response.data;
};

export const getLatestRisk = async (country = 'IND') => {
  const response = await api.get(`/interconnection/latest/${country}`);
  return response.data;
};

export const getRiskSummary = async (country = 'IND') => {
  const response = await api.get(`/interconnection/summary/${country}`);
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
