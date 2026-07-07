/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      fontFamily: {
        sans: ['Outfit', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
      colors: {
        lm: {
          bg:      '#f0f2f8',
          surface: '#ffffff',
          card:    'rgba(255,255,255,0.85)',
          border:  'rgba(0,0,0,0.07)',
          muted:   '#6b7280',
          text:    '#1e2030',
          indigo:  '#6366f1',
          blue:    '#3b82f6',
          red:     '#dc2626',
          green:   '#16a34a',
          amber:   '#d97706',
          purple:  '#9333ea',
        }
      }
    },
  },
  plugins: [],
}
