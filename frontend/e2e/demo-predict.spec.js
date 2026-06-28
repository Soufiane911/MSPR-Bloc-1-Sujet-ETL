import { expect, test } from "@playwright/test";

test("la demo predict affiche une prediction complete", async ({ page }) => {
  await page.goto("/demo-predict");

  await expect(page.getByRole("heading", { name: "Démo IA — Substitution Avion vers Train", level: 1 })).toBeVisible();
  await expect(page.getByLabel("Trajet avion")).toBeVisible();

  await page.getByRole("button", { name: "Lancer la prédiction" }).click();

  await expect(page.getByRole("heading", { name: "Résultat du modèle" })).toBeVisible({ timeout: 30000 });
  await expect(page.getByText("Classe prédite")).toBeVisible();
  await expect(page.getByText("Confiance")).toBeVisible();
  await expect(page.getByText("Gain CO2 estimé")).toBeVisible();
  await expect(page.getByText("Explication:")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Probabilités par classe" })).toBeVisible();
});