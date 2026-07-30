import { create } from 'zustand'
import type { ChartConfig } from '../types/dashboard'
import { loadCharts, saveCharts } from './storage'

interface DashboardsState {
  charts: ChartConfig[]
  addChart: (chart: Omit<ChartConfig, 'id'>) => void
  removeChart: (id: string) => void
}

export const useDashboardsStore = create<DashboardsState>((set, get) => ({
  charts: loadCharts(),

  addChart: (chart) => {
    const charts = [...get().charts, { ...chart, id: crypto.randomUUID() }]
    saveCharts(charts)
    set({ charts })
  },

  removeChart: (id) => {
    const charts = get().charts.filter((chart) => chart.id !== id)
    saveCharts(charts)
    set({ charts })
  },
}))
