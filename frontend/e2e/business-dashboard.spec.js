import { expect, test } from "@playwright/test";

test("le dashboard expose une lecture metier des trajets", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });

  // L'en-tete principal est toujours present, meme si les donnees partielles
  await expect(page.getByRole("heading", { name: "ObRail Europe", level: 1 })).toBeVisible();

  await page.getByRole("button", { name: "Trajets", exact: true }).click();
  await expect(page.getByRole("columnheader", { name: "Origine" })).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Destination" })).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Operateur" })).toBeVisible();

  await page.getByRole("button", { name: "Carte des trajets" }).click();
  await expect(page.getByText(/trajets cartographies|Aucun trajet cartographiable/)).toBeVisible();
});
