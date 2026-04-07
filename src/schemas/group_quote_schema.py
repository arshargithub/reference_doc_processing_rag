from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


# =========================
# SHARED BASE
# =========================

class BaseCoverage(BaseModel):
    benefit_requested: Optional[str] = Field(
        None,
        description="Indicates whether the benefit is requested or included. Typical values: 'Yes', 'No', 'Alternate', 'Included', 'Not Included'."
    )
    modular_flex: Optional[str] = Field(
        None,
        description="Indicates whether modular or flexible plan design applies. Values: 'Yes' or 'No'."
    )
    working_status: Optional[str] = Field(
        None,
        description="Eligibility requirement related to employee working status (e.g., 'Active', 'Full-time', 'Permanent')."
    )
    underwriting_method: Optional[str] = Field(
        None,
        description="Method used for underwriting the benefit (e.g., 'Fully Insured', 'ASO', 'Retention')."
    )
    waiting_period: Optional[str] = Field(
        None,
        description="Waiting period before benefit eligibility begins, typically expressed in months (e.g., '3 months', '90 days')."
    )
    benefit_year: Optional[str] = Field(
        None,
        description="Defines the benefit year structure (e.g., 'Calendar year', 'Policy year')."
    )


# =========================
# SALES & ADVISOR
# =========================

class SalesInformation(BaseModel):
    sales_representative: Optional[str] = Field(
        None,
        description="Name of the sales representative or account executive handling the quote.",
        json_schema_extra={
            "aliases": ["account executive", "sales rep"],
            "keywords": ["sales", "representative", "account executive"],
        },
    )
    request_details: Optional[str] = Field(
        None,
        description="Additional details or notes about the quote request or sales context."
    )


class AdvisorInformation(BaseModel):
    advisor_name: Optional[str] = Field(
        None,
        description="Name of the insurance advisor or broker.",
        json_schema_extra={
            "aliases": ["broker name", "consultant name"],
            "keywords": ["broker", "consultant", "advisor", "submitted by"],
            "chunk_type_hints": ["email_header", "kv_group"],
        },
    )
    brokerage: Optional[str] = Field(
        None,
        description="Name of the brokerage firm associated with the advisor.",
        json_schema_extra={
            "aliases": ["brokerage firm", "agency name"],
            "keywords": ["brokerage", "firm", "agency"],
        },
    )


class CommissionScale(BaseModel):
    life: Optional[str] = Field(None, description="Commission rate for life insurance benefits (e.g., '10%').")
    health: Optional[str] = Field(None, description="Commission rate for health benefits (e.g., '8%').")
    dental: Optional[str] = Field(None, description="Commission rate for dental benefits (e.g., '12%').")


class FlatCommission(BaseModel):
    amount: Optional[str] = Field(
        None,
        description="Flat commission amount if applicable, typically expressed as a percentage or dollar value."
    )


# =========================
# CLIENT INFO
# =========================

class ClientInformation(BaseModel):
    legal_name: Optional[str] = Field(
        None,
        description="Legal name of the company or organization.",
        json_schema_extra={
            "aliases": ["company name", "organization name", "group name"],
            "keywords": ["legal name", "company", "organization", "policyholder"],
            "chunk_type_hints": ["kv_group"],
        },
    )
    address: Optional[str] = Field(None, description="Primary business address of the company.")
    city: Optional[str] = Field(None, description="City where the company is located.")
    province: Optional[str] = Field(None, description="Province or state of the company location.")
    postal_code: Optional[str] = Field(None, description="Postal or ZIP code.")
    quote_number: Optional[str] = Field(None, description="Unique quote or reference number.")
    sic_code: Optional[str] = Field(None, description="Standard Industrial Classification (SIC) code.")
    group_size: Optional[str] = Field(
        None,
        description="Total number of employees in the group.",
        json_schema_extra={
            "keywords": ["number of employees", "headcount", "group size", "total employees"],
            "chunk_type_hints": ["kv_group"],
        },
    )
    nature_of_business: Optional[str] = Field(None, description="Description of the company's industry or operations.")


class UnderwritingQuestions(BaseModel):
    upsell_policy_number: Optional[str] = Field(None, description="Existing policy number if this is an upsell.")
    years_in_operation: Optional[str] = Field(None, description="Number of years the company has been operating.")
    employer_cost_percentage: Optional[str] = Field(None, description="Percentage of benefit cost paid by employer.")
    union_members: Optional[str] = Field(None, description="Indicates if employees are unionized (Yes/No).")
    laid_off_strike_lockout: Optional[str] = Field(None, description="Whether employees are affected by layoffs, strikes, or lockouts.")
    subsidiaries_covered: Optional[str] = Field(None, description="Indicates if subsidiaries are included in coverage.")
    previous_rbc_insurance: Optional[str] = Field(None, description="Whether the group was previously insured by RBC.")
    previous_rbc_gis_offer: Optional[str] = Field(None, description="Whether a prior RBC group insurance quote was provided.")
    reason_for_marketing: Optional[str] = Field(None, description="Reason for seeking a new quote or marketing the plan.")
    legal_status: Optional[str] = Field(None, description="Legal structure of the company (e.g., corporation, partnership).")
    is_rtq_alternate: Optional[str] = Field(None, description="Indicates if the request is an alternate quote scenario.")
    pooled_claims: Optional[str] = Field(None, description="Whether the group has pooled claims history.")
    spousal_cohabitation_period: Optional[str] = Field(None, description="Definition of spousal eligibility period.")


# =========================
# GROUP STRUCTURE
# =========================

class GroupClassDescription(BaseModel):
    number_of_divisions: Optional[str] = Field(None, description="Number of organizational divisions.")
    number_of_true_classes: Optional[str] = Field(None, description="Number of benefit classes.")
    modular_flex: Optional[str] = Field(None, description="Indicates if modular class structure is used.")
    class_descriptions: Optional[str] = Field(None, description="Comma-separated list of employee class names.")


# =========================
# PARAMEDICAL
# =========================

class ParamedicalPractitioner(BaseModel):
    practitioner_type: str = Field(..., description="Type of practitioner (e.g., Chiropractor, Physiotherapist).")
    group_coverage_type: Optional[str] = Field(None, description="Coverage grouping structure (e.g., combined or individual).")
    reimbursement_percentage: Optional[str] = Field(None, description="Reimbursement percentage (e.g., '80%').")
    annual_max: Optional[str] = Field(None, description="Annual maximum reimbursement amount.")
    annual_max_visits: Optional[str] = Field(None, description="Maximum number of visits per year.")
    per_visit_maximum: Optional[str] = Field(None, description="Maximum reimbursement per visit.")
    doctor_referral_required: Optional[str] = Field(None, description="Indicates if a doctor referral is required.")


class EHC_PM_Coverage(BaseModel):
    paramedical_practitioners: Optional[str] = Field(
        None,
        description="Comma-separated list of paramedical practitioner types covered (e.g., 'Chiropractor, Physiotherapist, Psychologist')."
    )
    paramedical_overall_applied_annual_max: Optional[str] = Field(
        None,
        description="Overall annual maximum applied across all paramedical services."
    )
    paramedical_per_practitioner_annual_max: Optional[str] = Field(
        None,
        description="Annual maximum per practitioner type."
    )
    paramedical_per_practitioner_annual_max_visits: Optional[str] = Field(
        None,
        description="Maximum visits allowed per practitioner annually."
    )


# =========================
# COVERAGES
# =========================

class BasicLifeCoverage(BaseCoverage):
    flat_or_multiple: Optional[str] = Field(None, description="Indicates if benefit is flat amount or salary multiple.")
    flat_amount: Optional[str] = Field(None, description="Flat dollar coverage amount.")
    multiple_of_salary: Optional[str] = Field(None, description="Salary multiple for coverage (e.g., '1x', '2x', '60% of earnings').")
    maximum_benefit: Optional[str] = Field(None, description="Maximum benefit cap.")
    current_nem: Optional[str] = Field(None, description="Current Non-Evidence Maximum.")
    benefit_reduction: Optional[str] = Field(None, description="Schedule for benefit reduction.")
    termination_age: Optional[str] = Field(None, description="Age when coverage terminates (e.g., '65', '70').")


class DentalCoverage(BaseModel):
    benefit_requested: Optional[str] = Field(None, description="Indicates if dental coverage is included.")
    fee_guide: Optional[str] = Field(None, description="Fee guide used (e.g., CURRENT, CURRENT LESS ONE).")
    overall_deductible: Optional[str] = Field(None, description="Overall deductible amount.")
    basic_preventive_reimbursement: Optional[str] = Field(None, description="Reimbursement % for basic/preventive services.")
    major_restorative_reimbursement: Optional[str] = Field(None, description="Reimbursement % for major services.")
    ortho_reimbursement: Optional[str] = Field(None, description="Orthodontics reimbursement percentage.")
    ortho_lifetime_maximum: Optional[str] = Field(None, description="Lifetime orthodontics maximum.")


class ExtendedHealthCareCoverage(BaseModel):
    benefit_requested: Optional[str] = Field(None, description="Indicates if EHC is included.")
    modular_flex: Optional[str] = Field(None, description="Indicates modular design.")
    working_status: Optional[str] = Field(None, description="Eligibility requirement.")
    waiting_period: Optional[str] = Field(None, description="Waiting period.")
    hospital_room_type: Optional[str] = Field(None, description="Hospital room coverage level.")
    overall_deductible: Optional[str] = Field(None, description="Overall deductible.")
    drugs_deductible: Optional[str] = Field(None, description="Drug deductible.")
    medical_services_deductible: Optional[str] = Field(None, description="Medical deductible.")
    paramedical: Optional[EHC_PM_Coverage] = Field(None, description="Paramedical coverage details.")


# =========================
# EMPLOYEE CLASS
# =========================

class EmployeeClass(BaseModel):
    model_config = {"extra": "allow"}

    class_name: str = Field(
        ...,
        description="Identifier for the employee class (e.g., Class A, Executives, Hourly).",
        json_schema_extra={
            "aliases": ["employee class", "benefit class", "class identifier"],
            "keywords": ["class", "division", "group", "plan design"],
        },
    )
    class_description: Optional[str] = Field(None, description="Description of eligible employees.")
    division: Optional[str] = Field(None, description="Division or location.")
    number_of_employees: Optional[str] = Field(None, description="Number of employees in class.")
    eligibility_criteria: Optional[str] = Field(None, description="Eligibility requirements.")

    basic_life: Optional[BasicLifeCoverage] = Field(
        None,
        json_schema_extra={
            "keywords": ["life insurance", "basic life", "AD&D", "life benefit"],
            "chunk_type_hints": ["kv_group", "table_chunk"],
        },
    )
    dental: Optional[DentalCoverage] = Field(
        None,
        json_schema_extra={
            "keywords": ["dental", "dental coverage", "fee guide", "orthodontic"],
            "chunk_type_hints": ["kv_group", "table_chunk"],
        },
    )
    extended_health_care: Optional[ExtendedHealthCareCoverage] = Field(
        None,
        json_schema_extra={
            "keywords": ["extended health", "EHC", "health care", "drug", "hospital"],
            "chunk_type_hints": ["kv_group", "table_chunk"],
        },
    )


# =========================
# ROOT
# =========================

class InsuranceExtraction(BaseModel):
    sales_information: Optional[SalesInformation] = None
    advisor_information: Optional[AdvisorInformation] = None
    commission_scale: Optional[CommissionScale] = None
    flat_commission: Optional[FlatCommission] = None

    client_information: Optional[ClientInformation] = None
    underwriting_questions: Optional[UnderwritingQuestions] = None

    group_class_description: Optional[GroupClassDescription] = None

    employee_classes: list[EmployeeClass] = Field(
        default_factory=list,
        description="List of employee classes and associated benefit designs.",
        json_schema_extra={
            "keywords": ["employee class", "benefit class", "plan design", "class A", "class B"],
            "chunk_type_hints": ["kv_group", "table_chunk"],
        },
    )

    supplementary_comments: Optional[str] = Field(None, description="Additional notes or special instructions.")
    alternate_options: Optional[list[str]] = Field(None, description="Alternate plan scenarios requested.")