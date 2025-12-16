import { render, screen } from '@testing-library/react';
import App from './App';

test('renders dashboard header', () => {
  render(<App />);
  const headerElement = screen.getByText(/Dashboard/i);
  expect(headerElement).toBeInTheDocument();
});

test('renders tab buttons', () => {
  render(<App />);
  const buttons = screen.getAllByRole('button');
  const buttonTexts = buttons.map(b => b.textContent);
  expect(buttonTexts).toContain('Health');
  expect(buttonTexts).toContain('Trajets');
  expect(buttonTexts).toContain('Statistiques');
  expect(buttonTexts).toContain('Import');
});
