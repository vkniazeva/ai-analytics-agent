import { useState } from 'react'
import { AddChartModal } from '../components/AddChartModal'
import { Button } from '../components/Button'
import { ChartCard } from '../components/ChartCard'
import { useDashboardsStore } from '../lib/dashboardsStore'

export function DashboardsView() {
  const { charts, addChart, removeChart } = useDashboardsStore()
  const [modalOpen, setModalOpen] = useState(false)

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-gray-900">Dashboards</h1>
        <Button onClick={() => setModalOpen(true)}>+ Add Chart</Button>
      </div>

      {charts.length === 0 && (
        <p className="text-sm text-gray-500">
          No charts yet. Click "+ Add Chart" to create one.
        </p>
      )}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        {charts.map((chart) => (
          <ChartCard
            key={chart.id}
            config={chart}
            onRemove={() => removeChart(chart.id)}
          />
        ))}
      </div>

      {modalOpen && (
        <AddChartModal
          onClose={() => setModalOpen(false)}
          onSubmit={(chart) => {
            addChart(chart)
            setModalOpen(false)
          }}
        />
      )}
    </div>
  )
}
