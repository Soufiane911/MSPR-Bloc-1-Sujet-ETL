import { expect, test } from "@playwright/test";

test("navigation entre les onglets principaux", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });

  await page.getByRole("button", { name: "Comparaison Jour/Nuit" }).click();
  await expect(page.getByRole("heading", { name: "Nombre de trains par pays" })).toBeVisible();

  await page.getByRole("button", { name: "Reseau & Distance" }).click();
  await expect(page.getByRole("heading", { name: "Routes les plus frequentes" })).toBeVisible();

  await page.getByRole("button", { name: "Carte" }).click();
  await expect(page.getByRole("heading", { name: "Carte des gares" })).toBeVisible();

  await page.getByRole("button", { name: "Qualite & Export" }).click();
  await expect(page.getByRole("heading", { name: "Qualite des donnees" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export CSV" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Excel" })).toBeVisible();
});
