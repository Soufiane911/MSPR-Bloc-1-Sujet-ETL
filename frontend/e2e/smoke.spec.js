import { expect, test } from "@playwright/test";

test("le dashboard charge et affiche les KPI", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "ObRail Europe", level: 1 })).toBeVisible();
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });
  await expect(page.getByText(/^Erreur:/)).toHaveCount(0);

  // Recherche exacte pour eviter les collisions avec les warnings de fetch
  await expect(page.getByText("Trains", { exact: true })).toBeVisible();
  await expect(page.getByText("Gares", { exact: true })).toBeVisible();
  await expect(page.getByText("Operateurs", { exact: true })).toBeVisible();
  await expect(page.getByText("Dessertes", { exact: true })).toBeVisible();
});
