import { expect, test } from "@playwright/test";

test("les filtres sont utilisables et conservent la saisie", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });

  const trainType = page.getByLabel("Type de train");
  await trainType.selectOption("Jour");
  await expect(trainType).toHaveValue("Jour");

  const minDistance = page.getByLabel("Distance minimale");
  const maxDistance = page.getByLabel("Distance maximale");

  await minDistance.fill("100");
  await maxDistance.fill("1200");

  await expect(minDistance).toHaveValue("100");
  await expect(maxDistance).toHaveValue("1200");

  await expect(page.getByText(/^Erreur:/)).toHaveCount(0);
});
