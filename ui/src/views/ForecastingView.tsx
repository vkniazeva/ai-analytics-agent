import {
  ChevronRight,
  Download,
  MousePointerClick,
  PlaneLanding,
  PlaneTakeoff,
  Recycle,
  Send,
  TrendingUp,
  Users,
  X,
  Zap,
} from 'lucide-react'
import { useState } from 'react'
import type { FormEvent } from 'react'
import { Button } from '../components/Button'
import { Card } from '../components/Card'
import { Drawer } from '../components/Drawer'
import { IconButton } from '../components/IconButton'
import { Input } from '../components/Input'
import { Select } from '../components/Select'
import { Spinner } from '../components/Spinner'
import {
  DEPART_WINDOWS,
  OBJECTIVES,
  objectiveDef,
  toApiThresholdType,
  toPredictRequest,
} from '../lib/forecastMapping'
import type { DepartWindow, Objective } from '../lib/forecastMapping'
import { postPredict, postPredictItemDetail } from '../lib/api'
import type { PredictItem, PredictItemDetail } from '../types/forecasting'

const OBJECTIVE_ICONS: Record<Objective, typeof Recycle> = {
  low_wastage: Recycle,
  no_lost_sales: TrendingUp,
}

function accuracyDotColor(accuracy: number | null): string {
  if (accuracy === null) return 'var(--text-faint)'
  if (accuracy >= 0.85) return 'var(--green-main)'
  if (accuracy >= 0.75) return 'var(--purple-main)'
  return 'var(--warning-500)'
}

function downloadCsv(rows: PredictItem[]) {
  const header = 'item_id,predicted_quantity'
  const body = rows.map((r) => `${r.item_id},${r.predicted_quantity}`).join('\n')
  const blob = new Blob([`${header}\n${body}`], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'forecast.csv'
  a.click()
  URL.revokeObjectURL(url)
}

export function ForecastingView() {
  const [origin, setOrigin] = useState('city_001')
  const [destination, setDestination] = useState('city_029')
  const [pax, setPax] = useState<number | ''>(214)
  const [depart, setDepart] = useState<DepartWindow>('morning')
  const [objective, setObjective] = useState<Objective>('low_wastage')

  const [results, setResults] = useState<PredictItem[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastBrief, setLastBrief] = useState<{
    origin: string
    destination: string
    pax: number
    depart: DepartWindow
    objective: Objective
  } | null>(null)

  const [selectedItemId, setSelectedItemId] = useState<string | null>(null)
  const [detail, setDetail] = useState<PredictItemDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)

  const objectiveInfo = objectiveDef(objective)
  const ObjectiveIcon = OBJECTIVE_ICONS[objective]

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    if (pax === '') return
    const brief = { origin, destination, pax, depart, objective }

    setLoading(true)
    setError(null)
    setResults(null)
    setSelectedItemId(null)
    setDetail(null)

    try {
      const request = toPredictRequest({ origin, destination, pax, depart })
      const response = await postPredict(toApiThresholdType(objective), request)
      setResults(response)
      setLastBrief(brief)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to get predictions')
    } finally {
      setLoading(false)
    }
  }

  async function handleRowClick(itemId: string) {
    if (!lastBrief) return
    setSelectedItemId(itemId)
    setDetail(null)
    setDetailError(null)
    setDetailLoading(true)

    try {
      const request = toPredictRequest(lastBrief)
      const response = await postPredictItemDetail(
        toApiThresholdType(lastBrief.objective),
        itemId,
        request,
      )
      setDetail(response)
    } catch (err) {
      setDetailError(
        err instanceof Error ? err.message : 'Failed to load item detail',
      )
    } finally {
      setDetailLoading(false)
    }
  }

  const totalUnits =
    results?.reduce((sum, item) => sum + item.predicted_quantity, 0) ?? 0

  return (
    <div className="h-full overflow-y-auto px-8 pb-10 pt-6">
      <div className="mx-auto flex max-w-[1100px] flex-col gap-5">
        <Card className="p-5 px-6">
          <form
            onSubmit={handleSubmit}
            className="grid items-end gap-3.5"
            style={{
              gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
            }}
          >
            <Input
              label="Origin"
              icon={<PlaneTakeoff size={18} />}
              value={origin}
              onChange={(e) => setOrigin(e.target.value)}
            />
            <Input
              label="Destination"
              icon={<PlaneLanding size={18} />}
              value={destination}
              onChange={(e) => setDestination(e.target.value)}
            />
            <Input
              label="Passengers"
              type="number"
              icon={<Users size={18} />}
              value={pax}
              onChange={(e) =>
                setPax(e.target.value === '' ? '' : Number(e.target.value))
              }
            />
            <Select
              label="Departure"
              value={depart}
              onChange={(e) => setDepart(e.target.value as DepartWindow)}
              options={DEPART_WINDOWS.map((d) => ({
                value: d.value,
                label: d.label,
              }))}
            />
            <Select
              label="Forecast objective"
              value={objective}
              onChange={(e) => setObjective(e.target.value as Objective)}
              options={OBJECTIVES.map((o) => ({ value: o.key, label: o.label }))}
            />
            <Button type="submit" icon={<Zap size={18} />} disabled={loading}>
              {loading ? 'Running...' : 'Run forecast'}
            </Button>
          </form>

          <div
            className="mt-[18px] flex items-center gap-[11px] rounded-xl p-3.5"
            style={{
              backgroundColor: objectiveInfo.tint,
              border: `1px solid ${objectiveInfo.border}`,
            }}
          >
            <ObjectiveIcon size={20} style={{ color: objectiveInfo.ink }} />
            <p className="flex-1 text-[13px] leading-[1.6] text-[var(--text-body)]">
              <strong style={{ color: objectiveInfo.ink }}>
                {objectiveInfo.label}
              </strong>
              {' - '}
              {objectiveInfo.copy}
            </p>
            <div className="text-right">
              <div className="text-[11.5px] text-[var(--text-muted)]">
                Threshold
              </div>
              <div className="text-[15px] font-bold text-[var(--text-strong)]">
                {objectiveInfo.threshold.toFixed(2)}
              </div>
            </div>
          </div>
        </Card>

        {error && <p className="text-sm text-[var(--danger-500)]">{error}</p>}

        {loading && (
          <div className="flex items-center gap-2">
            <Spinner />
            <span className="text-sm text-[var(--text-muted)]">
              Loading predictions...
            </span>
          </div>
        )}

        {results && lastBrief && (
          <Card className="overflow-hidden">
            <div className="flex items-start justify-between gap-3 px-6 pb-3.5 pt-[18px]">
              <div>
                <div className="text-[15px] font-bold text-[var(--text-strong)]">
                  Recommended load - {lastBrief.origin} to{' '}
                  {lastBrief.destination}
                </div>
                <div className="mt-0.5 text-[12.5px] text-[var(--text-muted)]">
                  {lastBrief.pax} passengers - {lastBrief.depart} departure -{' '}
                  {objectiveDef(lastBrief.objective).label.toLowerCase()}
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-xs font-semibold text-[var(--text-muted)]">
                <MousePointerClick size={16} />
                Click an item for forecast detail
              </div>
            </div>

            <div
              className="grid gap-4 border-y border-[var(--border-subtle)] bg-[var(--color-200)] px-6 py-2.5 text-[11px] font-bold uppercase tracking-[0.07em] text-[var(--text-muted)]"
              style={{ gridTemplateColumns: 'minmax(140px,1fr) 160px 34px' }}
            >
              <div>Item ID</div>
              <div className="text-right">Predicted quantity</div>
              <div />
            </div>

            {results.length === 0 && (
              <div className="px-6 py-6 text-center text-sm text-[var(--text-faint)]">
                No results
              </div>
            )}

            {results.map((item) => (
              <button
                key={item.item_id}
                type="button"
                onClick={() => handleRowClick(item.item_id)}
                className="grid w-full items-center gap-4 border-b border-[var(--border-subtle)] px-6 py-3.5 text-left hover:bg-[var(--color-200)]"
                style={{ gridTemplateColumns: 'minmax(140px,1fr) 160px 34px' }}
              >
                <div className="font-mono text-[14.5px] font-semibold text-[var(--text-strong)]">
                  {item.item_id}
                </div>
                <div className="text-right text-xl font-bold tabular-nums text-[var(--text-strong)]">
                  {item.predicted_quantity}
                </div>
                <ChevronRight size={20} className="text-[var(--text-faint)]" />
              </button>
            ))}

            <div className="flex items-center justify-between px-6 py-3.5">
              <span className="text-[12.5px] text-[var(--text-muted)]">
                {results.length} items - {totalUnits} units in total
              </span>
              <div className="flex gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  icon={<Download size={14} />}
                  onClick={() => downloadCsv(results)}
                >
                  CSV
                </Button>
                <Button
                  variant="secondary"
                  size="sm"
                  icon={<Send size={14} />}
                  disabled
                  title="Not available yet"
                >
                  Send to catering
                </Button>
              </div>
            </div>
          </Card>
        )}
      </div>

      <Drawer open={selectedItemId !== null} onClose={() => setSelectedItemId(null)}>
        <div className="flex items-start justify-between border-b border-[var(--border-subtle)] px-6 pb-[18px] pt-[22px]">
          <div>
            <div className="text-[11.5px] font-semibold uppercase tracking-[0.08em] text-[var(--text-muted)]">
              Forecast detail
            </div>
            <h2 className="mt-1 font-mono text-[22px] font-bold text-[var(--text-strong)]">
              {selectedItemId}
            </h2>
          </div>
          <IconButton aria-label="Close" onClick={() => setSelectedItemId(null)}>
            <X size={18} />
          </IconButton>
        </div>

        <div className="flex-1 overflow-y-auto px-6 py-6">
          {detailLoading && (
            <div className="flex items-center gap-2">
              <Spinner />
              <span className="text-sm text-[var(--text-muted)]">Loading...</span>
            </div>
          )}
          {detailError && (
            <p className="text-sm text-[var(--danger-500)]">{detailError}</p>
          )}

          {detail && (
            <div className="flex flex-col gap-6">
              <div className="rounded-2xl bg-[var(--color-800)] p-[22px] text-white">
                <div className="flex items-start justify-between">
                  <div>
                    <div className="text-[11.5px] uppercase text-white/55">
                      Predicted quantity
                    </div>
                    <div className="text-[46px] font-bold tracking-[-0.03em]">
                      {detail.predicted_quantity}
                    </div>
                    <div className="text-[12.5px] text-white/60">
                      units to load
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-[11.5px] text-white/55">
                      Historical average
                    </div>
                    <div className="text-[22px] font-bold">
                      {detail.historical_average}
                    </div>
                    {detail.estimated_accuracy !== null && (
                      <div className="mt-1.5 inline-flex items-center gap-[7px] rounded-[var(--radius-pill)] bg-white/12 px-2.5 py-1 text-[11.5px] font-semibold">
                        <span
                          className="h-[7px] w-[7px] rounded-full"
                          style={{
                            backgroundColor: accuracyDotColor(
                              detail.estimated_accuracy,
                            ),
                          }}
                        />
                        {Math.round(detail.estimated_accuracy * 100)}% estimated
                        accuracy
                      </div>
                    )}
                  </div>
                </div>
              </div>

              <div>
                <div className="mb-2 text-[11.5px] font-bold uppercase text-[var(--text-muted)]">
                  Model output
                </div>
                <div className="overflow-hidden rounded-[var(--radius-md)] border border-[var(--border-subtle)]">
                  {(
                    [
                      ['item_id', detail.item_id],
                      ['threshold_type', detail.threshold_type],
                      ['threshold_value', detail.threshold_value],
                      ['predicted_quantity', detail.predicted_quantity],
                      ['historical_average', detail.historical_average],
                      ['estimated_accuracy', detail.estimated_accuracy ?? '—'],
                    ] as const
                  ).map(([key, value], i) => (
                    <div
                      key={key}
                      className={`flex items-center justify-between px-4 py-[13px] ${i > 0 ? 'border-t border-[var(--border-subtle)]' : ''}`}
                    >
                      <span className="font-mono text-[13px] text-[var(--text-muted)]">
                        {key}
                      </span>
                      <span className="tabular-nums text-[13.5px] font-semibold text-[var(--text-strong)]">
                        {value}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      </Drawer>
    </div>
  )
}
