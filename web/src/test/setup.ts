// Vitest setup: registers Testing Library's jest-dom matchers (toBeInTheDocument
// and friends) and unmounts rendered trees between tests.
import '@testing-library/jest-dom/vitest'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

afterEach(() => {
  cleanup()
})
