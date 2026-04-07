"""Intent definitions for the sample insurance domain."""

INTENTS: list[str] = [
    "group_insurance_quote_request",
    "insurance_claim",
    "wire_transfer_request",
    "invoice_payment_request",
]

INTENT_DESCRIPTIONS: dict[str, str] = {
    "group_insurance_quote_request": (
        "A request from a broker or advisor to obtain a group insurance quote "
        "for a corporate client.  Typically includes an employee census, plan "
        "design specifications, and existing coverage details."
    ),
    "insurance_claim": (
        "A submission of an insurance claim for processing, including supporting "
        "documentation and claimant information."
    ),
    "wire_transfer_request": (
        "A request to initiate a wire transfer, with beneficiary and payment details."
    ),
    "invoice_payment_request": (
        "A request to process payment for an invoice, with invoice details and "
        "payment instructions."
    ),
}
