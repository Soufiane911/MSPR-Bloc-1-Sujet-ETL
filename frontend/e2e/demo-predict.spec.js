import { expect, test } from "@playwright/test";

const API_URL = /http:\/\/(?:localhost|127\.0\.0\.1):8000\/.*/;

async function fulfillJson(route, status, body) {
  await route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

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

test("la demo ignore une alternative train incoherente et utilise le fallback coherent", async ({ page }) => {
  await page.route(API_URL, async (route) => {
    const url = new URL(route.request().url());

    if (url.pathname === "/aviationStats/topRoutes") {
      await fulfillJson(route, 200, [
        {
          id: "FCO-PRG",
          origin_iata: "FCO",
          destination_iata: "PRG",
          origin_name: "Rome",
          destination_name: "Prague",
          origin_country: "IT",
          destination_country: "CZ",
          avg_distance_km: 935,
          estimated_duration_min: 110,
          airline_count: 7,
          airlines: "AZ,IB,OK,QS,U2,VY,W6",
        },
      ]);
      return;
    }

    if (url.pathname === "/trajets") {
      await fulfillJson(route, 200, [
        {
          trajet_id: 10,
          train_number: "back_on_track_UZ 099",
          operator_name: "Укрзалізниця",
          train_type: "night",
          origin: "Одеса Головна",
          destination: "Buhuși",
          origin_country: "EU",
          destination_country: "EU",
          distance_km: 1036,
          duration_min: 13,
        },
      ]);
      return;
    }

    if (url.pathname === "/predict/") {
      await fulfillJson(route, 200, {
        prediction: "fort_potentiel",
        confidence: 0.91,
        probabilities: { fort_potentiel: 0.91, potentiel_moyen: 0.08, faible_potentiel: 0.01 },
        explanation: "Prediction fort_potentiel basee sur : liaison internationale.",
        model_version: "test",
      });
      return;
    }

    await fulfillJson(route, 404, { detail: `Unexpected route: ${url.pathname}` });
  });

  await page.goto("/demo-predict");
  await page.getByRole("button", { name: "Lancer la prédiction" }).click();

  const trainCard = page
    .getByRole("heading", { name: "Alternative train proposée" })
    .locator("..");

  await expect(trainCard.getByText("Train: DEMO-ALT (ObRail Synthétique)")).toBeVisible();
  await expect(trainCard.getByText("Route: Rome -> Prague")).toBeVisible();
  await expect(trainCard.getByText("Alternative estimée faute de trajet ferroviaire cohérent dans la base.")).toBeVisible();
  await expect(page.getByText("Одеса Головна")).toHaveCount(0);
});
