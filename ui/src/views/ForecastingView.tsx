import { useState } from 'react'
import type { FormEvent } from 'react'
import { Button } from '../components/Button'
import { Card } from '../components/Card'
import { Spinner } from '../components/Spinner'
import { postPredict, postPredictItemDetail } from '../lib/api'
import type {
  DayPeriod,
  PredictItem,
  PredictItemDetail,
  PredictRequest,
  ThresholdType,
} from '../types/forecasting'

const DAY_PERIODS: DayPeriod[] = ['Morning', 'Day', 'Evening', 'Night']
const THRESHOLD_TYPES: ThresholdType[] = ['low_missed_sales', 'low_wastage']

export function ForecastingView() {
  const [route, setRoute] = useState('')
  const [passengers, setPassengers] = useState<number | ''>('')
  const [dayPeriod, setDayPeriod] = useState<DayPeriod>('Morning')
  const [thresholdType, setThresholdType] =
    useState<ThresholdType>('low_missed_sales')

  const [results, setResults] = useState<PredictItem[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [selectedItemId, setSelectedItemId] = useState<string | null>(null)
  const [detail, setDetail] = useState<PredictItemDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)

  function currentRequest(): PredictRequest | null {
    if (!route.trim() || passengers === '') return null
    return { route: route.trim(), expected_pax: passengers, day_period: dayPeriod }
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    const request = currentRequest()
    if (!request) return

    setLoading(true)
    setError(null)
    setResults(null)
    setSelectedItemId(null)
    setDetail(null)

    try {
      const response = await postPredict(thresholdType, request)
      setResults(response)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to get predictions')
    } finally {
      setLoading(false)
    }
  }

  async function handleRowClick(itemId: string) {
    const request = currentRequest()
    if (!request) return

    setSelectedItemId(itemId)
    setDetail(null)
    setDetailError(null)
    setDetailLoading(true)

    try {
      const response = await postPredictItemDetail(thresholdType, itemId, request)
      setDetail(response)
    } catch (err) {
      setDetailError(
        err instanceof Error ? err.message : 'Failed to load item detail',
      )
    } finally {
      setDetailLoading(false)
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-xl font-semibold text-gray-900">Forecasting</h1>

      <Card>
        <form
          onSubmit={handleSubmit}
          className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4"
        >
          <label className="flex flex-col gap-1 text-sm font-medium text-gray-700">
            Route
            <input
              type="text"
              value={route}
              onChange={(e) => setRoute(e.target.value)}
              placeholder="e.g. JFK-LHR"
              className="rounded-md border border-gray-300 px-3 py-2 text-sm"
              required
            />
          </label>

          <label className="flex flex-col gap-1 text-sm font-medium text-gray-700">
            Passengers
            <input
              type="number"
              min={0}
              value={passengers}
              onChange={(e) =>
                setPassengers(e.target.value === '' ? '' : Number(e.target.value))
              }
              className="rounded-md border border-gray-300 px-3 py-2 text-sm"
              required
            />
          </label>

          <label className="flex flex-col gap-1 text-sm font-medium text-gray-700">
            Day Period
            <select
              value={dayPeriod}
              onChange={(e) => setDayPeriod(e.target.value as DayPeriod)}
              className="rounded-md border border-gray-300 px-3 py-2 text-sm"
            >
              {DAY_PERIODS.map((period) => (
                <option key={period} value={period}>
                  {period}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 text-sm font-medium text-gray-700">
            Threshold Type
            <select
              value={thresholdType}
              onChange={(e) => setThresholdType(e.target.value as ThresholdType)}
              className="rounded-md border border-gray-300 px-3 py-2 text-sm"
            >
              {THRESHOLD_TYPES.map((type) => (
                <option key={type} value={type}>
                  {type}
                </option>
              ))}
            </select>
          </label>

          <div className="sm:col-span-2 lg:col-span-4">
            <Button type="submit" disabled={loading}>
              {loading ? 'Predicting...' : 'Predict'}
            </Button>
          </div>
        </form>
      </Card>

      {error && <p className="text-sm text-red-600">{error}</p>}

      {loading && (
        <div className="flex items-center gap-2">
          <Spinner />
          <span className="text-sm text-gray-500">Loading predictions...</span>
        </div>
      )}

      {results && (
        <Card className="overflow-x-auto p-0">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-gray-200 text-gray-500">
              <tr>
                <th className="px-4 py-2 font-medium">Item ID</th>
                <th className="px-4 py-2 font-medium">Predicted Quantity</th>
              </tr>
            </thead>
            <tbody>
              {results.length === 0 && (
                <tr>
                  <td colSpan={2} className="px-4 py-6 text-center text-gray-400">
                    No results
                  </td>
                </tr>
              )}
              {results.map((item) => (
                <tr
                  key={item.item_id}
                  onClick={() => handleRowClick(item.item_id)}
                  className={`cursor-pointer border-b border-gray-100 hover:bg-gray-50 ${
                    selectedItemId === item.item_id ? 'bg-blue-50' : ''
                  }`}
                >
                  <td className="px-4 py-2 text-gray-800">{item.item_id}</td>
                  <td className="px-4 py-2 text-gray-800">
                    {item.predicted_quantity}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}

      {selectedItemId && (
        <Card className="flex flex-col gap-2">
          <h2 className="text-sm font-semibold text-gray-900">
            Details for {selectedItemId}
          </h2>
          {detailLoading && (
            <div className="flex items-center gap-2">
              <Spinner />
              <span className="text-sm text-gray-500">Loading...</span>
            </div>
          )}
          {detailError && <p className="text-sm text-red-600">{detailError}</p>}
          {detail && (
            <dl className="grid grid-cols-2 gap-2 text-sm sm:grid-cols-4">
              <div>
                <dt className="text-gray-500">Predicted Quantity</dt>
                <dd className="text-gray-900">{detail.predicted_quantity}</dd>
              </div>
              <div>
                <dt className="text-gray-500">Threshold Value</dt>
                <dd className="text-gray-900">{detail.threshold_value}</dd>
              </div>
              <div>
                <dt className="text-gray-500">Historical Average</dt>
                <dd className="text-gray-900">{detail.historical_average}</dd>
              </div>
              <div>
                <dt className="text-gray-500">Estimated Accuracy</dt>
                <dd className="text-gray-900">
                  {detail.estimated_accuracy ?? '—'}
                </dd>
              </div>
            </dl>
          )}
        </Card>
      )}
    </div>
  )
}
