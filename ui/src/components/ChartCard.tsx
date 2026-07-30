import { useEffect, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { getDashboardMetrics } from '../lib/api'
import type { ChartConfig } from '../types/dashboard'
import { Card } from './Card'
import { Spinner } from './Spinner'

const SERIES_COLORS = [
  '#2a78d6',
  '#eb6834',
  '#1baf7a',
  '#eda100',
  '#e87ba4',
  '#008300',
  '#4a3aa7',
  '#e34948',
]

interface ChartCardProps {
  config: ChartConfig
  onRemove: () => void
}

export function ChartCard({ config, onRemove }: ChartCardProps) {
  const [rows, setRows] = useState<Record<string, string | number>[] | null>(
    null,
  )
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setRows(null)
    setError(null)

    getDashboardMetrics(config.domain, config.metrics, config.groupBy)
      .then((response) => {
        if (!cancelled) setRows(response.rows)
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load chart')
        }
      })

    return () => {
      cancelled = true
    }
  }, [config.domain, config.metrics, config.groupBy])

  const xAxisKey = config.groupBy[0]
  const Chart = config.chartType === 'bar' ? BarChart : LineChart

  return (
    <Card className="flex flex-col gap-3">
      <div className="flex items-start justify-between gap-2">
        <h3 className="text-sm font-semibold text-gray-900">
          {config.title}
        </h3>
        <button
          type="button"
          aria-label="Remove chart"
          onClick={onRemove}
          className="text-gray-400 hover:text-gray-600"
        >
          ×
        </button>
      </div>

      {error && <p className="text-sm text-red-600">{error}</p>}

      {!error && !rows && (
        <div className="flex items-center justify-center gap-2 py-16">
          <Spinner />
          <span className="text-sm text-gray-500">Loading...</span>
        </div>
      )}

      {!error && rows && rows.length === 0 && (
        <p className="py-16 text-center text-sm text-gray-400">No data</p>
      )}

      {!error && rows && rows.length > 0 && (
        <ResponsiveContainer width="100%" height={260}>
          <Chart data={rows}>
            <CartesianGrid stroke="#e1e0d9" strokeDasharray="3 3" />
            <XAxis
              dataKey={xAxisKey}
              stroke="#898781"
              tick={{ fontSize: 12, fill: '#52514e' }}
            />
            <YAxis stroke="#898781" tick={{ fontSize: 12, fill: '#52514e' }} />
            <Tooltip />
            {config.metrics.length > 1 && <Legend />}
            {config.metrics.map((metric, index) =>
              config.chartType === 'bar' ? (
                <Bar
                  key={metric}
                  dataKey={metric}
                  fill={SERIES_COLORS[index % SERIES_COLORS.length]}
                  radius={[4, 4, 0, 0]}
                />
              ) : (
                <Line
                  key={metric}
                  type="monotone"
                  dataKey={metric}
                  stroke={SERIES_COLORS[index % SERIES_COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
              ),
            )}
          </Chart>
        </ResponsiveContainer>
      )}
    </Card>
  )
}
