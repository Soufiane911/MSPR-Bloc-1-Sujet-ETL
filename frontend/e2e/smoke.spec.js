import { expect, test } from "@playwright/test";

test("le dashboard charge et affiche les KPI", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "ObRail Europe", level: 1 })).toBeVisible();
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });
  await expect(page.getByText(/^Erreur:/)).toHaveCount(0);

  await expect(page.getByText("Trains")).toBeVisible();
  await expect(page.getByText("Gares")).toBeVisible();
  await expect(page.getByText("Operateurs")).toBeVisible();
  await expect(page.getByText("Dessertes")).toBeVisible();
});
