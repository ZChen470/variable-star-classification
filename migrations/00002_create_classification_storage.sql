-- +goose Up

CREATE TABLE classification_runs (
    run_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE,

    object_id TEXT NOT NULL,
    candidate_revision BIGINT NOT NULL,
    light_curve_revision BIGINT NOT NULL,
    effective_epoch_count INTEGER NOT NULL,

    execution_mode SMALLINT NOT NULL,

    coarse_source_type SMALLINT NOT NULL,
    coarse_source_run_id UUID NULL
        REFERENCES classification_runs (run_id)
        ON DELETE RESTRICT,
    coarse_source_light_curve_revision BIGINT NOT NULL,
    coarse_source_epoch_count INTEGER NOT NULL,
    xgboost_executed BOOLEAN NOT NULL,

    model_bundle_version TEXT NOT NULL,

    coarse_probabilities REAL[] NOT NULL,
    fine_conditional_probabilities REAL[] NOT NULL,
    leaf_probabilities REAL[] NOT NULL,

    predicted_coarse_class INTEGER NOT NULL,
    predicted_leaf_class INTEGER NOT NULL,

    completed_at TIMESTAMPTZ NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT classification_runs_object_id_check
        CHECK (
            object_id <> ''
            AND object_id = btrim(object_id)
        ),

    CONSTRAINT classification_runs_candidate_revision_check
        CHECK (candidate_revision > 0),

    CONSTRAINT classification_runs_light_curve_revision_check
        CHECK (light_curve_revision > 0),

    CONSTRAINT classification_runs_effective_epoch_count_check
        CHECK (effective_epoch_count >= 3),

    CONSTRAINT classification_runs_execution_mode_check
        CHECK (execution_mode IN (1, 2, 3)),

    CONSTRAINT classification_runs_coarse_source_type_check
        CHECK (coarse_source_type IN (1, 2, 3)),

    CONSTRAINT classification_runs_coarse_source_revision_check
        CHECK (coarse_source_light_curve_revision > 0),

    CONSTRAINT classification_runs_coarse_source_epoch_count_check
        CHECK (coarse_source_epoch_count >= 3),

    CONSTRAINT classification_runs_coarse_source_not_self_check
        CHECK (
            coarse_source_run_id IS NULL
            OR coarse_source_run_id <> run_id
        ),

    CONSTRAINT classification_runs_coarse_source_consistency_check
        CHECK (
            (
                coarse_source_type = 1
                AND xgboost_executed
                AND coarse_source_run_id IS NULL
                AND coarse_source_light_curve_revision =
                    light_curve_revision
                AND coarse_source_epoch_count =
                    effective_epoch_count
            )
            OR
            (
                coarse_source_type = 2
                AND NOT xgboost_executed
                AND coarse_source_run_id IS NOT NULL
                AND coarse_source_light_curve_revision <
                    light_curve_revision
            )
            OR
            (
                coarse_source_type = 3
                AND xgboost_executed
                AND coarse_source_run_id IS NULL
                AND coarse_source_light_curve_revision =
                    light_curve_revision
                AND coarse_source_epoch_count =
                    effective_epoch_count
            )
        ),

    CONSTRAINT classification_runs_model_bundle_version_check
        CHECK (
            model_bundle_version <> ''
            AND model_bundle_version = btrim(model_bundle_version)
        ),

    CONSTRAINT classification_runs_taxonomy_version_check
        CHECK (
            taxonomy_version <> ''
            AND taxonomy_version = btrim(taxonomy_version)
        ),

    CONSTRAINT classification_runs_preprocessing_version_check
        CHECK (
            preprocessing_version <> ''
            AND preprocessing_version = btrim(preprocessing_version)
        ),

    CONSTRAINT classification_runs_feature_schema_version_check
        CHECK (
            feature_schema_version <> ''
            AND feature_schema_version =
                btrim(feature_schema_version)
        ),

    CONSTRAINT classification_runs_tensor_schema_version_check
        CHECK (
            tensor_schema_version <> ''
            AND tensor_schema_version =
                btrim(tensor_schema_version)
        ),

    CONSTRAINT classification_runs_coarse_probabilities_shape_check
        CHECK (
            array_ndims(coarse_probabilities) = 1
            AND array_lower(coarse_probabilities, 1) = 1
            AND cardinality(coarse_probabilities) = 7
            AND array_position(
                coarse_probabilities,
                NULL::REAL
            ) IS NULL
        ),

    CONSTRAINT classification_runs_fine_probabilities_shape_check
        CHECK (
            array_ndims(fine_conditional_probabilities) = 1
            AND array_lower(fine_conditional_probabilities, 1) = 1
            AND cardinality(fine_conditional_probabilities) = 10
            AND array_position(
                fine_conditional_probabilities,
                NULL::REAL
            ) IS NULL
        ),

    CONSTRAINT classification_runs_leaf_probabilities_shape_check
        CHECK (
            array_ndims(leaf_probabilities) = 1
            AND array_lower(leaf_probabilities, 1) = 1
            AND cardinality(leaf_probabilities) = 12
            AND array_position(
                leaf_probabilities,
                NULL::REAL
            ) IS NULL
        ),

    CONSTRAINT classification_runs_predicted_coarse_class_check
        CHECK (
            predicted_coarse_class IN (
                10,
                20,
                30,
                40,
                50,
                60,
                70
            )
        ),

    CONSTRAINT classification_runs_predicted_leaf_class_check
        CHECK (
            predicted_leaf_class IN (
                30,
                70,
                1001,
                1002,
                2001,
                2002,
                4001,
                4002,
                5001,
                5002,
                6001,
                6002
            )
        )
);

CREATE INDEX classification_runs_object_revision_idx
    ON classification_runs (
        object_id,
        light_curve_revision DESC
    );

CREATE INDEX classification_runs_compatible_coarse_lookup_idx
    ON classification_runs (
        object_id,
        model_bundle_version,
        light_curve_revision DESC
    )
    WHERE xgboost_executed;

CREATE TABLE current_classifications (
    object_id TEXT PRIMARY KEY,

    run_id UUID NOT NULL UNIQUE
        REFERENCES classification_runs (run_id)
        ON DELETE RESTRICT,

    light_curve_revision BIGINT NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT current_classifications_object_id_check
        CHECK (
            object_id <> ''
            AND object_id = btrim(object_id)
        ),

    CONSTRAINT current_classifications_revision_check
        CHECK (light_curve_revision > 0)
);

COMMENT ON COLUMN classification_runs.coarse_probabilities IS
    'Order: ROTATING, CATACLYSMIC, ECLIPSING_BINARY, LONG_PERIOD, PULSATING, RR_LYRAE, SUPERNOVA';

COMMENT ON COLUMN classification_runs.fine_conditional_probabilities IS
    'Order: EW, EA, BY_DRA, RS_CVN, RRAB, RRC, SR, MIRA, DSCT, CEP';

COMMENT ON COLUMN classification_runs.leaf_probabilities IS
    'Order: EW, EA, BY_DRA, RS_CVN, RRAB, RRC, SR, MIRA, DSCT, CEP, CATACLYSMIC, SUPERNOVA';


-- +goose Down

DROP TABLE current_classifications;
DROP TABLE classification_runs;
