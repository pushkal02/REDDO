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
        cyber: {
          black: '#030407',
          slate: '#090d16',
          card: 'rgba(9, 13, 22, 0.75)',
          cyan: '#00f0ff',
          purple: '#bd00ff',
          red: '#ff0055',
          green: '#00ff66',
          amber: '#ffaa00',
          border: 'rgba(255, 255, 255, 0.05)',
        }
      }
    },
  },
  plugins: [],
}
