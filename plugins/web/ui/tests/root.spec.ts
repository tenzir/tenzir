import { test, expect } from '@playwright/test';

test('root/home page has title', async ({ page }) => {
  // await page.goto('https://playwright.dev/');
  await page.goto('http://localhost:5173/');

  // Expect a title "to contain" a substring.
  await expect(page).toHaveTitle(/VAST/);
});
