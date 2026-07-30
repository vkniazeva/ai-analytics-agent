import { useEffect, useState } from 'react'
import { getDashboardMetadata } from '../lib/api'
import type {
  ChartConfig,
  ChartType,
  DashboardMetadataResponse,
  Domain,
} from '../types/dashboard'
import { Button } from './Button'
import { Spinner } from './Spinner'

const DOMAINS: Domain[] = [
  'sales',
  'wastage',
  'flights',
  'product_catalog',
  'pax_sales',
]

const CHART_TYPES: ChartType[] = ['line', 'bar']

interface AddChartModalProps {
  onClose: () => void
  onSubmit: (chart: Omit<ChartConfig, 'id'>) => void
}

export function AddChartModal({ onClose, onSubmit }: AddChartModalProps) {
  const [metadata, setMetadata] = useState<DashboardMetadataResponse | null>(
    null,
  )
  const [metadataError, setMetadataError] = useState<string | null>(null)
  const [domain, setDomain] = useState<Domain>('sales')
  const [metrics, setMetrics] = useState<string[]>([])
  const [groupBy, setGroupBy] = useState<string[]>([])
  const [chartType, setChartType] = useState<ChartType>('line')

  useEffect(() => {
    getDashboardMetadata()
      .then(setMetadata)
      .catch((err) =>
        setMetadataError(
          err instanceof Error ? err.message : 'Failed to load metadata',
        ),
      )
  }, [])

  const availableMetrics = metadata?.metrics[domain] ?? []
  const availableDimensions = metadata?.dimensions[domain] ?? []

  function toggle(list: string[], value: string): string[] {
    return list.includes(value)
      ? list.filter((item) => item !== value)
      : [...list, value]
  }

  function handleDomainChange(nextDomain: Domain) {
    setDomain(nextDomain)
    setMetrics([])
    setGroupBy([])
  }

  function handleSubmit() {
    if (metrics.length === 0) return
    onSubmit({
      title: `${domain} · ${metrics.join(', ')}`,
      domain,
      metrics,
      groupBy,
      chartType,
    })
  }

  return (
    <div className="fixed inset-0 flex items-center justify-center bg-black/40 p-4">
      <div className="w-full max-w-md rounded-lg bg-white p-6 shadow-lg">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">
          Add Chart
        </h2>

        {metadataError && (
          <p className="mb-3 text-sm text-red-600">{metadataError}</p>
        )}

        {!metadata && !metadataError && (
          <div className="flex items-center gap-2 py-6">
            <Spinner />
            <span className="text-sm text-gray-500">Loading options...</span>
          </div>
        )}

        {metadata && (
          <div className="flex flex-col gap-4">
            <label className="flex flex-col gap-1 text-sm font-medium text-gray-700">
              Domain
              <select
                className="rounded-md border border-gray-300 px-3 py-2 text-sm"
                value={domain}
                onChange={(e) =>
                  handleDomainChange(e.target.value as Domain)
                }
              >
                {DOMAINS.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </label>

            <fieldset className="flex flex-col gap-1">
              <legend className="text-sm font-medium text-gray-700">
                Metrics
              </legend>
              <div className="flex max-h-32 flex-col gap-1 overflow-y-auto rounded-md border border-gray-200 p-2">
                {availableMetrics.length === 0 && (
                  <span className="text-sm text-gray-400">
                    No metrics for this domain
                  </span>
                )}
                {availableMetrics.map((metric) => (
                  <label
                    key={metric}
                    className="flex items-center gap-2 text-sm text-gray-700"
                  >
                    <input
                      type="checkbox"
                      checked={metrics.includes(metric)}
                      onChange={() => setMetrics(toggle(metrics, metric))}
                    />
                    {metric}
                  </label>
                ))}
              </div>
            </fieldset>

            <fieldset className="flex flex-col gap-1">
              <legend className="text-sm font-medium text-gray-700">
                Group By
              </legend>
              <div className="flex max-h-32 flex-col gap-1 overflow-y-auto rounded-md border border-gray-200 p-2">
                {availableDimensions.length === 0 && (
                  <span className="text-sm text-gray-400">
                    No dimensions for this domain
                  </span>
                )}
                {availableDimensions.map((dimension) => (
                  <label
                    key={dimension}
                    className="flex items-center gap-2 text-sm text-gray-700"
                  >
                    <input
                      type="checkbox"
                      checked={groupBy.includes(dimension)}
                      onChange={() => setGroupBy(toggle(groupBy, dimension))}
                    />
                    {dimension}
                  </label>
                ))}
              </div>
            </fieldset>

            <label className="flex flex-col gap-1 text-sm font-medium text-gray-700">
              Chart Type
              <select
                className="rounded-md border border-gray-300 px-3 py-2 text-sm"
                value={chartType}
                onChange={(e) => setChartType(e.target.value as ChartType)}
              >
                {CHART_TYPES.map((type) => (
                  <option key={type} value={type}>
                    {type}
                  </option>
                ))}
              </select>
            </label>

            {metrics.length === 0 && (
              <p className="text-sm text-amber-600">
                Select at least one metric.
              </p>
            )}
          </div>
        )}

        <div className="mt-6 flex justify-end gap-2">
          <Button variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={metrics.length === 0}>
            Add Chart
          </Button>
        </div>
      </div>
    </div>
  )
}
