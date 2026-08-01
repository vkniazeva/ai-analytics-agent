import type { ButtonHTMLAttributes } from 'react'

export function IconButton({
  className = '',
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type="button"
      className={`flex h-[34px] w-[34px] items-center justify-center rounded-[10px] border border-[var(--border-subtle)] text-[var(--text-body)] transition-colors duration-150 ease-[var(--ease-standard)] hover:bg-ink-200 ${className}`}
      {...props}
    />
  )
}
