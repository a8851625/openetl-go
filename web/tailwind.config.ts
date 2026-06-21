import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['JetBrains Mono', 'Menlo', 'Monaco', 'monospace'],
      },
    },
  },
  safelist: [
    'badge-emerald', 'badge-blue', 'badge-amber', 'badge-rose',
    'badge-slate', 'badge-violet', 'badge-cyan', 'badge-indigo',
    'bg-emerald-500', 'bg-blue-500', 'bg-rose-500', 'bg-slate-400',
    'bg-indigo-500', 'bg-amber-500', 'bg-cyan-500',
    'text-emerald-600', 'text-blue-600', 'text-rose-600', 'text-amber-600',
  ],
} satisfies Config;
