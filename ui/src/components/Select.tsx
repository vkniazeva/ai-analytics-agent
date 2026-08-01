import type { ReactNode, SelectHTMLAttributes } from 'react'

interface SelectOption {
  value: string
  label: string
}

interface SelectProps extends SelectHTMLAttributes<HTMLSelectElement> {
  label?: string
  icon?: ReactNode
  options: SelectOption[]
}

export function Select({
  label,
  icon,
  options,
  className = '',
  id,
  ...props
}: SelectProps) {
  return (
    <label className="flex flex-col gap-1.5" htmlFor={id}>
      {label && (
        <span className="text-sm font-medium text-[var(--text-body)]">
          {label}
        </span>
      )}
      <div className="relative flex items-center">
        {icon && (
          <span className="pointer-events-none absolute left-3 flex items-center text-[var(--text-faint)]">
            {icon}
          </span>
        )}
        <select
          id={id}
          className={`h-[46px] w-full appearance-none rounded-[var(--radius-md)] border-[1.5px] border-[var(--border-default)] bg-white text-sm text-[var(--text-strong)] outline-none transition-colors duration-150 ease-[var(--ease-standard)] focus:border-purple-main focus:ring-[3px] focus:ring-[var(--ring-color)] ${icon ? 'pl-10 pr-3' : 'px-3'} ${className}`}
          {...props}
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>
    </label>
  )
}
