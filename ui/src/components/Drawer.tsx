import { useEffect } from 'react'
import type { ReactNode } from 'react'

interface DrawerProps {
  open: boolean
  onClose: () => void
  children: ReactNode
}

export function Drawer({ open, onClose, children }: DrawerProps) {
  useEffect(() => {
    if (!open) return
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [open, onClose])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-40">
      <div
        className="absolute inset-0 backdrop-blur-[2px]"
        style={{ backgroundColor: 'var(--scrim)' }}
        onClick={onClose}
      />
      <div
        className="absolute right-0 top-0 z-50 flex h-full w-[440px] flex-col border-l border-[var(--border-subtle)] bg-white shadow-[var(--shadow-lg)]"
        style={{ animation: 'omSlide 0.28s var(--ease-out)' }}
      >
        {children}
      </div>
    </div>
  )
}
