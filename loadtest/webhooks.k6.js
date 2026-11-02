// Load test for the webhook endpoint: unique transactions plus a share of duplicate retries.
//   k6 run -e BASE_URL=https://<your-api>.up.railway.app loadtest/webhooks.k6.js
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const accepted = new Counter("webhooks_accepted");

export const options = {
  scenarios: {
    webhooks: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "15s", target: 200 },
        { duration: "45s", target: 200 },
        { duration: "10s", target: 0 },
      ],
    },
  },
  thresholds: {
    "http_req_duration{endpoint:webhook}": ["p(95)<500", "p(99)<1000"],
    "checks{endpoint:webhook}": ["rate==1.0"],
    http_req_failed: ["rate<0.001"],
  },
};

export default function () {
  // ~20% of requests replay a recent id to exercise idempotency under load.
  const duplicate = Math.random() < 0.2 && __ITER > 0;
  const id = duplicate ? `txn_k6_${__VU}_${__ITER - 1}` : `txn_k6_${__VU}_${__ITER}`;
  const res = http.post(
    `${BASE_URL}/v1/webhooks/transactions`,
    JSON.stringify({
      transaction_id: id,
      source_account: "acc_user_789",
      destination_account: "acc_merchant_456",
      amount: 1500,
      currency: "INR",
    }),
    { headers: { "Content-Type": "application/json" }, tags: { endpoint: "webhook" } },
  );
  if (check(res, { "202 Accepted": (r) => r.status === 202 }, { endpoint: "webhook" })) {
    accepted.add(1);
  }
}
