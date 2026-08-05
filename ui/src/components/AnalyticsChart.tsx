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

export const SERIES_COLORS = [
  '#8a5a9e',
  '#5c1f72',
  '#9d7fb0',
  '#6e3f7d',
  '#b9a0c4',
  '#4a1760',
  '#a688b8',
  '#7a5e87',
]

interface AnalyticsChartProps {
  chartType: 'bar' | 'line'
  xKey: string
  seriesKeys: string[]
  data: Record<string, string | number>[]
  height?: number
  showLegend?: boolean
}

export function AnalyticsChart({
  chartType,
  xKey,
  seriesKeys,
  data,
  height = 260,
  showLegend = true,
}: AnalyticsChartProps) {
  const Chart = chartType === 'bar' ? BarChart : LineChart

  return (
    <ResponsiveContainer width="100%" height={height}>
      <Chart data={data}>
        <CartesianGrid stroke="#e1e0d9" strokeDasharray="3 3" />
        <XAxis
          dataKey={xKey}
          stroke="#898781"
          tick={{ fontSize: 12, fill: '#52514e' }}
        />
        <YAxis stroke="#898781" tick={{ fontSize: 12, fill: '#52514e' }} />
        <Tooltip />
        {showLegend && seriesKeys.length > 1 && <Legend />}
        {seriesKeys.map((key, index) =>
          chartType === 'bar' ? (
            <Bar
              key={key}
              dataKey={key}
              fill={SERIES_COLORS[index % SERIES_COLORS.length]}
              radius={[4, 4, 0, 0]}
            />
          ) : (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={SERIES_COLORS[index % SERIES_COLORS.length]}
              strokeWidth={2}
              dot={{ r: 3 }}
            />
          ),
        )}
      </Chart>
    </ResponsiveContainer>
  )
}
