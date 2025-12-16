"""
Sri Lanka Weather Analytics - ML Dataset Preparation for May Evapotranspiration Prediction

This module prepares the dataset for machine learning prediction of evapotranspiration
values in May. It filters data for May months, selects relevant features, handles
missing values with mean imputation, and trains regression models.

Requirements: 5.2, 5.3
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, mean as spark_mean, month, isnan, isnull
)
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import os


# Feature columns for ML model (Requirements 5.2)
FEATURE_COLUMNS = [
    "precipitation_hours",
    "sunshine_duration", 
    "wind_speed_10m_max"
]

# Target column for prediction (Requirements 5.3)
TARGET_COLUMN = "et0_fao_evapotranspiration"

# May month number
MAY_MONTH = 5


def filter_may_data(df: DataFrame) -> DataFrame:
    """
    Filter the weather DataFrame to include only May months.
    
    Args:
        df: Weather DataFrame with 'month' column
        
    Returns:
        DataFrame containing only May records
    """
    return df.filter(col("month") == MAY_MONTH)


def select_ml_columns(df: DataFrame) -> DataFrame:
    """
    Select only the columns needed for ML: features and target.
    
    Also includes location_id and year for potential grouping/analysis.
    
    Args:
        df: Weather DataFrame
        
    Returns:
        DataFrame with only ML-relevant columns
    """
    columns_to_select = ["location_id", "year"] + FEATURE_COLUMNS + [TARGET_COLUMN]
    return df.select(*columns_to_select)


def get_column_means(df: DataFrame, columns: list) -> dict:
    """
    Calculate mean values for specified columns, excluding nulls and NaNs.
    
    Args:
        df: DataFrame
        columns: List of column names to calculate means for
        
    Returns:
        Dictionary mapping column names to their mean values
    """
    means = {}
    for column in columns:
        mean_value = df.filter(
            ~(isnan(col(column)) | isnull(col(column)))
        ).agg(spark_mean(col(column))).collect()[0][0]
        means[column] = mean_value if mean_value is not None else 0.0
    return means


def impute_missing_values_manual(df: DataFrame, columns: list) -> DataFrame:
    """
    Impute missing values (null and NaN) with column means.
    
    This is a manual implementation that calculates means and replaces
    null/NaN values directly.
    
    Args:
        df: DataFrame with potential missing values
        columns: List of columns to impute
        
    Returns:
        DataFrame with missing values replaced by column means
    """
    # Calculate means for all columns
    means = get_column_means(df, columns)
    
    # Replace nulls and NaNs with means
    result_df = df
    for column in columns:
        mean_value = means[column]
        result_df = result_df.withColumn(
            column,
            when(
                isnan(col(column)) | isnull(col(column)),
                mean_value
            ).otherwise(col(column))
        )
    
    return result_df


def impute_missing_values_ml(spark: SparkSession, df: DataFrame, columns: list) -> DataFrame:
    """
    Impute missing values using Spark ML Imputer with mean strategy.
    
    This uses the Spark ML Imputer which is more efficient for large datasets.
    
    Args:
        spark: SparkSession
        df: DataFrame with potential missing values
        columns: List of columns to impute
        
    Returns:
        DataFrame with missing values replaced by column means
    """
    # Create output column names (same as input for in-place replacement)
    output_columns = [f"{c}_imputed" for c in columns]
    
    imputer = Imputer(
        inputCols=columns,
        outputCols=output_columns,
        strategy="mean"
    )
    
    # Fit and transform
    model = imputer.fit(df)
    imputed_df = model.transform(df)
    
    # Replace original columns with imputed values and drop temp columns
    for orig, imputed in zip(columns, output_columns):
        imputed_df = imputed_df.withColumn(orig, col(imputed)).drop(imputed)
    
    return imputed_df


def prepare_ml_dataset(
    df: DataFrame,
    use_ml_imputer: bool = False,
    spark: SparkSession = None
) -> DataFrame:
    """
    Prepare the complete ML dataset for May evapotranspiration prediction.
    
    This function:
    1. Filters data for May months only
    2. Selects features and target columns
    3. Handles missing values with mean imputation
    
    Args:
        df: Weather DataFrame with parsed date and month columns
        use_ml_imputer: If True, use Spark ML Imputer; otherwise use manual imputation
        spark: SparkSession (required if use_ml_imputer is True)
        
    Returns:
        Prepared DataFrame ready for ML training/validation split
    """
    # Step 1: Filter for May months only
    may_df = filter_may_data(df)
    
    # Step 2: Select ML-relevant columns
    ml_df = select_ml_columns(may_df)
    
    # Step 3: Handle missing values with mean imputation
    columns_to_impute = FEATURE_COLUMNS + [TARGET_COLUMN]
    
    if use_ml_imputer and spark is not None:
        result_df = impute_missing_values_ml(spark, ml_df, columns_to_impute)
    else:
        result_df = impute_missing_values_manual(ml_df, columns_to_impute)
    
    return result_df


def get_dataset_statistics(df: DataFrame) -> dict:
    """
    Get statistics about the prepared ML dataset.
    
    Args:
        df: Prepared ML DataFrame
        
    Returns:
        Dictionary with dataset statistics
    """
    stats = {
        "total_records": df.count(),
        "null_counts": {},
        "feature_stats": {}
    }
    
    # Count nulls in each column
    for column in FEATURE_COLUMNS + [TARGET_COLUMN]:
        null_count = df.filter(
            isnan(col(column)) | isnull(col(column))
        ).count()
        stats["null_counts"][column] = null_count
    
    # Get basic statistics for features and target
    summary = df.select(FEATURE_COLUMNS + [TARGET_COLUMN]).summary().collect()
    stat_names = ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"]
    
    for i, stat_name in enumerate(stat_names):
        if i < len(summary):
            row = summary[i]
            for j, column in enumerate(FEATURE_COLUMNS + [TARGET_COLUMN]):
                if column not in stats["feature_stats"]:
                    stats["feature_stats"][column] = {}
                stats["feature_stats"][column][stat_name] = row[j + 1]
    
    return stats


def validate_ml_dataset(df: DataFrame) -> dict:
    """
    Validate the prepared ML dataset for completeness and quality.
    
    Args:
        df: Prepared ML DataFrame
        
    Returns:
        Dictionary with validation results
    """
    validation = {
        "is_valid": True,
        "issues": [],
        "record_count": df.count()
    }
    
    # Check for remaining nulls after imputation
    for column in FEATURE_COLUMNS + [TARGET_COLUMN]:
        null_count = df.filter(
            isnan(col(column)) | isnull(col(column))
        ).count()
        if null_count > 0:
            validation["is_valid"] = False
            validation["issues"].append(
                f"Column '{column}' still has {null_count} null/NaN values after imputation"
            )
    
    # Check for minimum record count
    if validation["record_count"] < 100:
        validation["issues"].append(
            f"Dataset has only {validation['record_count']} records, which may be insufficient for ML"
        )
    
    # Check for reasonable value ranges
    # ET0 should typically be between 0 and 15 mm/day
    et0_outliers = df.filter(
        (col(TARGET_COLUMN) < 0) | (col(TARGET_COLUMN) > 15)
    ).count()
    if et0_outliers > 0:
        validation["issues"].append(
            f"Found {et0_outliers} records with ET0 values outside typical range (0-15 mm)"
        )
    
    return validation


# Constants for train/validation split (Requirements 5.1)
TRAIN_RATIO = 0.8
VALIDATION_RATIO = 0.2
DEFAULT_SEED = 42  # Fixed seed for reproducibility


def split_train_validation(
    df: DataFrame,
    train_ratio: float = TRAIN_RATIO,
    validation_ratio: float = VALIDATION_RATIO,
    seed: int = DEFAULT_SEED
) -> tuple:
    """
    Split the dataset into training and validation sets.
    
    Uses Spark's randomSplit with a fixed seed for reproducibility.
    The split is approximately 80% training and 20% validation.
    
    Args:
        df: Prepared ML DataFrame
        train_ratio: Proportion of data for training (default 0.8)
        validation_ratio: Proportion of data for validation (default 0.2)
        seed: Random seed for reproducibility (default 42)
        
    Returns:
        Tuple of (training_df, validation_df)
        
    Requirements: 5.1
    """
    # Validate ratios sum to 1.0
    if abs((train_ratio + validation_ratio) - 1.0) > 0.001:
        raise ValueError(
            f"Train ratio ({train_ratio}) and validation ratio ({validation_ratio}) "
            f"must sum to 1.0"
        )
    
    # Use randomSplit with seed for reproducibility
    train_df, validation_df = df.randomSplit(
        weights=[train_ratio, validation_ratio],
        seed=seed
    )
    
    return train_df, validation_df


def verify_split_ratios(
    total_count: int,
    train_count: int,
    validation_count: int,
    expected_train_ratio: float = TRAIN_RATIO,
    tolerance: float = 0.01
) -> dict:
    """
    Verify that the actual split ratios are within acceptable tolerance.
    
    According to Property 9 (Training/Validation Split Ratio), the training
    set size divided by total dataset size should be approximately 0.8
    (within ±0.01 tolerance).
    
    Args:
        total_count: Total number of records in original dataset
        train_count: Number of records in training set
        validation_count: Number of records in validation set
        expected_train_ratio: Expected training ratio (default 0.8)
        tolerance: Acceptable deviation from expected ratio (default 0.01)
        
    Returns:
        Dictionary with verification results including:
        - is_valid: Boolean indicating if split is within tolerance
        - actual_train_ratio: Actual training set ratio
        - actual_validation_ratio: Actual validation set ratio
        - deviation: Absolute deviation from expected ratio
        - message: Human-readable verification message
        
    Requirements: 5.1
    """
    # Calculate actual ratios
    actual_train_ratio = train_count / total_count if total_count > 0 else 0.0
    actual_validation_ratio = validation_count / total_count if total_count > 0 else 0.0
    
    # Calculate deviation from expected
    deviation = abs(actual_train_ratio - expected_train_ratio)
    
    # Check if within tolerance
    is_valid = deviation <= tolerance
    
    # Verify counts add up
    counts_match = (train_count + validation_count) == total_count
    
    result = {
        "is_valid": is_valid and counts_match,
        "actual_train_ratio": actual_train_ratio,
        "actual_validation_ratio": actual_validation_ratio,
        "expected_train_ratio": expected_train_ratio,
        "deviation": deviation,
        "tolerance": tolerance,
        "total_count": total_count,
        "train_count": train_count,
        "validation_count": validation_count,
        "counts_match": counts_match
    }
    
    if is_valid and counts_match:
        result["message"] = (
            f"Split verification PASSED: Training ratio {actual_train_ratio:.4f} "
            f"is within ±{tolerance} of expected {expected_train_ratio}"
        )
    else:
        issues = []
        if not is_valid:
            issues.append(
                f"Training ratio {actual_train_ratio:.4f} deviates by {deviation:.4f} "
                f"from expected {expected_train_ratio} (tolerance: ±{tolerance})"
            )
        if not counts_match:
            issues.append(
                f"Record counts don't match: {train_count} + {validation_count} != {total_count}"
            )
        result["message"] = "Split verification FAILED: " + "; ".join(issues)
    
    return result


def prepare_train_validation_datasets(
    df: DataFrame,
    seed: int = DEFAULT_SEED
) -> dict:
    """
    Prepare complete training and validation datasets with verification.
    
    This is a convenience function that:
    1. Splits the data into training and validation sets
    2. Verifies the split ratios are within tolerance
    3. Returns both datasets along with verification results
    
    Args:
        df: Prepared ML DataFrame (after filtering and imputation)
        seed: Random seed for reproducibility
        
    Returns:
        Dictionary containing:
        - train_df: Training DataFrame
        - validation_df: Validation DataFrame
        - verification: Split ratio verification results
        - statistics: Basic statistics about the split
        
    Requirements: 5.1
    """
    # Get total count before split
    total_count = df.count()
    
    # Perform the split
    train_df, validation_df = split_train_validation(df, seed=seed)
    
    # Cache the DataFrames for efficient counting
    train_df.cache()
    validation_df.cache()
    
    # Get counts
    train_count = train_df.count()
    validation_count = validation_df.count()
    
    # Verify split ratios
    verification = verify_split_ratios(total_count, train_count, validation_count)
    
    return {
        "train_df": train_df,
        "validation_df": validation_df,
        "verification": verification,
        "statistics": {
            "total_records": total_count,
            "training_records": train_count,
            "validation_records": validation_count,
            "seed": seed
        }
    }


# Default hyperparameters for regression models
DEFAULT_MAX_ITER = 100
DEFAULT_REG_PARAM = 0.01
DEFAULT_ELASTIC_NET_PARAM = 0.0  # L2 regularization by default
DEFAULT_NUM_TREES = 20
DEFAULT_MAX_DEPTH = 5


def create_feature_vector(df: DataFrame, feature_cols: list = None) -> DataFrame:
    """
    Create a feature vector column from individual feature columns.
    
    Uses VectorAssembler to combine feature columns into a single 'features' vector
    required by MLlib models.
    
    Args:
        df: DataFrame with feature columns
        feature_cols: List of feature column names (default: FEATURE_COLUMNS)
        
    Returns:
        DataFrame with additional 'features' column containing the feature vector
        
    Requirements: 5.2
    """
    if feature_cols is None:
        feature_cols = FEATURE_COLUMNS
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"  # Skip rows with null/NaN in features
    )
    
    return assembler.transform(df)


def train_linear_regression(
    train_df: DataFrame,
    feature_cols: list = None,
    target_col: str = TARGET_COLUMN,
    max_iter: int = DEFAULT_MAX_ITER,
    reg_param: float = DEFAULT_REG_PARAM,
    elastic_net_param: float = DEFAULT_ELASTIC_NET_PARAM
) -> dict:
    """
    Train a Linear Regression model for evapotranspiration prediction.
    
    Uses Spark MLlib's LinearRegression with configurable hyperparameters.
    The model predicts et0_fao_evapotranspiration based on precipitation_hours,
    sunshine_duration, and wind_speed_10m_max features.
    
    Args:
        train_df: Training DataFrame with feature and target columns
        feature_cols: List of feature column names (default: FEATURE_COLUMNS)
        target_col: Target column name (default: TARGET_COLUMN)
        max_iter: Maximum number of iterations (default: 100)
        reg_param: Regularization parameter (default: 0.01)
        elastic_net_param: ElasticNet mixing parameter (0=L2, 1=L1, default: 0.0)
        
    Returns:
        Dictionary containing:
        - model: Trained LinearRegressionModel
        - coefficients: Model coefficients for each feature
        - intercept: Model intercept
        - training_summary: Training summary with metrics
        - hyperparameters: Dictionary of hyperparameters used
        
    Requirements: 5.3
    """
    if feature_cols is None:
        feature_cols = FEATURE_COLUMNS
    
    # Create feature vector
    train_with_features = create_feature_vector(train_df, feature_cols)
    
    # Configure Linear Regression model
    lr = LinearRegression(
        featuresCol="features",
        labelCol=target_col,
        predictionCol="prediction",
        maxIter=max_iter,
        regParam=reg_param,
        elasticNetParam=elastic_net_param,
        standardization=True,  # Standardize features before training
        fitIntercept=True
    )
    
    # Train the model
    lr_model = lr.fit(train_with_features)
    
    # Extract training summary
    training_summary = lr_model.summary
    
    # Build result dictionary
    result = {
        "model": lr_model,
        "model_type": "LinearRegression",
        "coefficients": dict(zip(feature_cols, lr_model.coefficients.toArray())),
        "intercept": lr_model.intercept,
        "training_summary": {
            "rmse": training_summary.rootMeanSquaredError,
            "r2": training_summary.r2,
            "mae": training_summary.meanAbsoluteError,
            "explained_variance": training_summary.explainedVariance,
            "num_iterations": training_summary.totalIterations
        },
        "hyperparameters": {
            "max_iter": max_iter,
            "reg_param": reg_param,
            "elastic_net_param": elastic_net_param
        },
        "feature_columns": feature_cols,
        "target_column": target_col
    }
    
    return result


def train_random_forest_regressor(
    train_df: DataFrame,
    feature_cols: list = None,
    target_col: str = TARGET_COLUMN,
    num_trees: int = DEFAULT_NUM_TREES,
    max_depth: int = DEFAULT_MAX_DEPTH,
    seed: int = DEFAULT_SEED
) -> dict:
    """
    Train a Random Forest Regressor model for evapotranspiration prediction.
    
    Uses Spark MLlib's RandomForestRegressor with configurable hyperparameters.
    Random Forest is an ensemble method that can capture non-linear relationships.
    
    Args:
        train_df: Training DataFrame with feature and target columns
        feature_cols: List of feature column names (default: FEATURE_COLUMNS)
        target_col: Target column name (default: TARGET_COLUMN)
        num_trees: Number of trees in the forest (default: 20)
        max_depth: Maximum depth of each tree (default: 5)
        seed: Random seed for reproducibility (default: 42)
        
    Returns:
        Dictionary containing:
        - model: Trained RandomForestRegressionModel
        - feature_importances: Importance scores for each feature
        - num_trees: Number of trees in the model
        - hyperparameters: Dictionary of hyperparameters used
        
    Requirements: 5.3
    """
    if feature_cols is None:
        feature_cols = FEATURE_COLUMNS
    
    # Create feature vector
    train_with_features = create_feature_vector(train_df, feature_cols)
    
    # Configure Random Forest Regressor
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=target_col,
        predictionCol="prediction",
        numTrees=num_trees,
        maxDepth=max_depth,
        seed=seed,
        featureSubsetStrategy="auto"
    )
    
    # Train the model
    rf_model = rf.fit(train_with_features)
    
    # Build result dictionary
    result = {
        "model": rf_model,
        "model_type": "RandomForestRegressor",
        "feature_importances": dict(zip(feature_cols, rf_model.featureImportances.toArray())),
        "num_trees": rf_model.getNumTrees,
        "hyperparameters": {
            "num_trees": num_trees,
            "max_depth": max_depth,
            "seed": seed
        },
        "feature_columns": feature_cols,
        "target_column": target_col
    }
    
    return result


def evaluate_model(
    model_result: dict,
    validation_df: DataFrame
) -> dict:
    """
    Evaluate a trained model on the validation dataset.
    
    Calculates RMSE and R-squared metrics as required by Requirements 5.4.
    
    Args:
        model_result: Dictionary returned by train_linear_regression or 
                      train_random_forest_regressor
        validation_df: Validation DataFrame
        
    Returns:
        Dictionary containing:
        - rmse: Root Mean Squared Error
        - r2: R-squared (coefficient of determination)
        - mae: Mean Absolute Error
        - predictions_df: DataFrame with predictions
        
    Requirements: 5.4
    """
    model = model_result["model"]
    feature_cols = model_result["feature_columns"]
    target_col = model_result["target_column"]
    
    # Create feature vector for validation data
    validation_with_features = create_feature_vector(validation_df, feature_cols)
    
    # Make predictions
    predictions_df = model.transform(validation_with_features)
    
    # Calculate RMSE
    rmse_evaluator = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = rmse_evaluator.evaluate(predictions_df)
    
    # Calculate R-squared
    r2_evaluator = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="r2"
    )
    r2 = r2_evaluator.evaluate(predictions_df)
    
    # Calculate MAE
    mae_evaluator = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="mae"
    )
    mae = mae_evaluator.evaluate(predictions_df)
    
    return {
        "rmse": rmse,
        "r2": r2,
        "mae": mae,
        "predictions_df": predictions_df,
        "model_type": model_result["model_type"]
    }


def train_and_evaluate_models(
    train_df: DataFrame,
    validation_df: DataFrame,
    feature_cols: list = None,
    target_col: str = TARGET_COLUMN
) -> dict:
    """
    Train both Linear Regression and Random Forest models and compare performance.
    
    This is a convenience function that trains both model types and returns
    comparative evaluation results.
    
    Args:
        train_df: Training DataFrame
        validation_df: Validation DataFrame
        feature_cols: List of feature column names (default: FEATURE_COLUMNS)
        target_col: Target column name (default: TARGET_COLUMN)
        
    Returns:
        Dictionary containing results for both models and comparison
        
    Requirements: 5.3, 5.4
    """
    if feature_cols is None:
        feature_cols = FEATURE_COLUMNS
    
    # Train Linear Regression
    print("Training Linear Regression model...")
    lr_result = train_linear_regression(
        train_df, 
        feature_cols=feature_cols,
        target_col=target_col
    )
    
    # Evaluate Linear Regression
    print("Evaluating Linear Regression model...")
    lr_eval = evaluate_model(lr_result, validation_df)
    
    # Train Random Forest
    print("Training Random Forest Regressor model...")
    rf_result = train_random_forest_regressor(
        train_df,
        feature_cols=feature_cols,
        target_col=target_col
    )
    
    # Evaluate Random Forest
    print("Evaluating Random Forest Regressor model...")
    rf_eval = evaluate_model(rf_result, validation_df)
    
    # Determine best model based on RMSE
    best_model = "LinearRegression" if lr_eval["rmse"] < rf_eval["rmse"] else "RandomForestRegressor"
    
    return {
        "linear_regression": {
            "training": lr_result,
            "evaluation": lr_eval
        },
        "random_forest": {
            "training": rf_result,
            "evaluation": rf_eval
        },
        "comparison": {
            "best_model": best_model,
            "lr_rmse": lr_eval["rmse"],
            "rf_rmse": rf_eval["rmse"],
            "lr_r2": lr_eval["r2"],
            "rf_r2": rf_eval["r2"]
        }
    }


def print_model_summary(model_result: dict, evaluation_result: dict = None):
    """
    Print a formatted summary of the trained model.
    
    Args:
        model_result: Dictionary returned by training function
        evaluation_result: Optional evaluation results dictionary
    """
    print(f"\n{'='*60}")
    print(f"Model: {model_result['model_type']}")
    print(f"{'='*60}")
    
    print(f"\nFeatures: {model_result['feature_columns']}")
    print(f"Target: {model_result['target_column']}")
    
    print(f"\nHyperparameters:")
    for param, value in model_result['hyperparameters'].items():
        print(f"  - {param}: {value}")
    
    if model_result['model_type'] == 'LinearRegression':
        print(f"\nModel Coefficients:")
        for feature, coef in model_result['coefficients'].items():
            print(f"  - {feature}: {coef:.6f}")
        print(f"  - intercept: {model_result['intercept']:.6f}")
        
        if 'training_summary' in model_result:
            print(f"\nTraining Metrics:")
            summary = model_result['training_summary']
            print(f"  - RMSE: {summary['rmse']:.4f}")
            print(f"  - R²: {summary['r2']:.4f}")
            print(f"  - MAE: {summary['mae']:.4f}")
            print(f"  - Iterations: {summary['num_iterations']}")
    
    elif model_result['model_type'] == 'RandomForestRegressor':
        print(f"\nFeature Importances:")
        for feature, importance in model_result['feature_importances'].items():
            print(f"  - {feature}: {importance:.4f}")
    
    if evaluation_result:
        print(f"\nValidation Metrics:")
        print(f"  - RMSE: {evaluation_result['rmse']:.4f}")
        print(f"  - R²: {evaluation_result['r2']:.4f}")
        print(f"  - MAE: {evaluation_result['mae']:.4f}")


# Target ET0 threshold for May 2026 prediction (Requirements 5.5)
TARGET_ET0_THRESHOLD = 1.5


def predict_conditions_for_low_et0(
    model_result: dict,
    train_df: DataFrame,
    target_et0: float = TARGET_ET0_THRESHOLD
) -> dict:
    """
    Predict weather conditions that would result in ET0 below the target threshold.
    
    Uses the trained model to find feature values that would predict
    evapotranspiration below 1.5mm for May 2026.
    
    For Linear Regression, we can solve for feature values analytically.
    For Random Forest, we use the training data to find similar conditions.
    
    Args:
        model_result: Dictionary returned by training function
        train_df: Training DataFrame to analyze historical patterns
        target_et0: Target ET0 threshold (default: 1.5mm)
        
    Returns:
        Dictionary containing:
        - predicted_precipitation_hours: Mean precipitation hours for low ET0
        - predicted_sunshine_duration: Mean sunshine duration for low ET0
        - predicted_wind_speed: Mean wind speed for low ET0
        - method: Method used for prediction
        - confidence_note: Note about prediction confidence
        
    Requirements: 5.5
    """
    from pyspark.sql.functions import avg
    
    # Find historical records with ET0 below threshold
    low_et0_records = train_df.filter(col(TARGET_COLUMN) < target_et0)
    low_et0_count = low_et0_records.count()
    
    if low_et0_count > 0:
        # Calculate mean feature values from historical low ET0 records
        feature_means = low_et0_records.agg(
            avg(col("precipitation_hours")).alias("mean_precipitation_hours"),
            avg(col("sunshine_duration")).alias("mean_sunshine_duration"),
            avg(col("wind_speed_10m_max")).alias("mean_wind_speed"),
            avg(col(TARGET_COLUMN)).alias("mean_et0")
        ).collect()[0]
        
        result = {
            "predicted_precipitation_hours": round(feature_means["mean_precipitation_hours"], 2),
            "predicted_sunshine_duration": round(feature_means["mean_sunshine_duration"], 2),
            "predicted_wind_speed": round(feature_means["mean_wind_speed"], 2),
            "historical_mean_et0": round(feature_means["mean_et0"], 4),
            "historical_sample_count": low_et0_count,
            "target_et0_threshold": target_et0,
            "method": "historical_analysis",
            "confidence_note": f"Based on {low_et0_count} historical May records with ET0 < {target_et0}mm"
        }
    else:
        # No historical records below threshold - use model coefficients for Linear Regression
        if model_result['model_type'] == 'LinearRegression':
            # For linear regression: ET0 = intercept + sum(coef_i * feature_i)
            # We want ET0 < 1.5, so we need to find feature combinations
            # Use the minimum observed values as a starting point
            
            feature_stats = train_df.agg(
                avg(col("precipitation_hours")).alias("mean_precipitation_hours"),
                avg(col("sunshine_duration")).alias("mean_sunshine_duration"),
                avg(col("wind_speed_10m_max")).alias("mean_wind_speed")
            ).collect()[0]
            
            # Adjust based on coefficient signs to minimize ET0
            coefficients = model_result['coefficients']
            intercept = model_result['intercept']
            
            # Start with mean values and adjust
            pred_precip = feature_stats["mean_precipitation_hours"]
            pred_sunshine = feature_stats["mean_sunshine_duration"]
            pred_wind = feature_stats["mean_wind_speed"]
            
            # Increase features with negative coefficients, decrease those with positive
            if coefficients.get("precipitation_hours", 0) > 0:
                pred_precip = max(0, pred_precip * 0.5)  # Reduce precipitation
            else:
                pred_precip = pred_precip * 1.5  # Increase precipitation
                
            if coefficients.get("sunshine_duration", 0) > 0:
                pred_sunshine = max(0, pred_sunshine * 0.5)  # Reduce sunshine
            else:
                pred_sunshine = pred_sunshine * 1.5  # Increase sunshine
                
            if coefficients.get("wind_speed_10m_max", 0) > 0:
                pred_wind = max(0, pred_wind * 0.5)  # Reduce wind
            else:
                pred_wind = pred_wind * 1.5  # Increase wind
            
            result = {
                "predicted_precipitation_hours": round(pred_precip, 2),
                "predicted_sunshine_duration": round(pred_sunshine, 2),
                "predicted_wind_speed": round(pred_wind, 2),
                "historical_sample_count": 0,
                "target_et0_threshold": target_et0,
                "method": "coefficient_analysis",
                "confidence_note": "Estimated using model coefficients - no historical records below threshold"
            }
        else:
            # For Random Forest, use percentile-based approach
            from pyspark.sql.functions import percentile_approx
            
            # Get 10th percentile values (lower values tend to have lower ET0)
            percentiles = train_df.agg(
                percentile_approx("precipitation_hours", 0.1).alias("p10_precipitation"),
                percentile_approx("sunshine_duration", 0.1).alias("p10_sunshine"),
                percentile_approx("wind_speed_10m_max", 0.1).alias("p10_wind")
            ).collect()[0]
            
            result = {
                "predicted_precipitation_hours": round(percentiles["p10_precipitation"], 2),
                "predicted_sunshine_duration": round(percentiles["p10_sunshine"], 2),
                "predicted_wind_speed": round(percentiles["p10_wind"], 2),
                "historical_sample_count": 0,
                "target_et0_threshold": target_et0,
                "method": "percentile_analysis",
                "confidence_note": "Estimated using 10th percentile values - no historical records below threshold"
            }
    
    return result


def generate_may_2026_prediction(
    spark: SparkSession,
    model_result: dict,
    predicted_conditions: dict
) -> DataFrame:
    """
    Generate a prediction DataFrame for May 2026 with the predicted conditions.
    
    Creates a DataFrame with the predicted feature values and uses the model
    to predict the expected ET0 value.
    
    Args:
        spark: SparkSession
        model_result: Dictionary returned by training function
        predicted_conditions: Dictionary from predict_conditions_for_low_et0
        
    Returns:
        DataFrame with prediction for May 2026
        
    Requirements: 5.5
    """
    from pyspark.sql import Row
    from pyspark.ml.linalg import Vectors
    
    # Create a DataFrame with the predicted conditions
    prediction_data = [(
        2026,  # year
        5,     # month (May)
        predicted_conditions["predicted_precipitation_hours"],
        predicted_conditions["predicted_sunshine_duration"],
        predicted_conditions["predicted_wind_speed"]
    )]
    
    columns = ["year", "month", "precipitation_hours", "sunshine_duration", "wind_speed_10m_max"]
    prediction_df = spark.createDataFrame(prediction_data, columns)
    
    # Create feature vector
    prediction_with_features = create_feature_vector(prediction_df, FEATURE_COLUMNS)
    
    # Make prediction using the model
    model = model_result["model"]
    result_df = model.transform(prediction_with_features)
    
    return result_df


def print_may_2026_prediction(predicted_conditions: dict, predicted_et0: float = None):
    """
    Print a formatted summary of the May 2026 prediction.
    
    Args:
        predicted_conditions: Dictionary from predict_conditions_for_low_et0
        predicted_et0: Optional predicted ET0 value from model
        
    Requirements: 5.5
    """
    print(f"\n{'='*60}")
    print("May 2026 Prediction for ET0 < 1.5mm")
    print(f"{'='*60}")
    
    print(f"\nTarget: Evapotranspiration (ET0) < {predicted_conditions['target_et0_threshold']}mm")
    print(f"Method: {predicted_conditions['method']}")
    print(f"Note: {predicted_conditions['confidence_note']}")
    
    print(f"\nPredicted Weather Conditions for May 2026:")
    print(f"  - Precipitation Hours: {predicted_conditions['predicted_precipitation_hours']:.2f} hours")
    print(f"  - Sunshine Duration: {predicted_conditions['predicted_sunshine_duration']:.2f} seconds")
    print(f"  - Wind Speed (10m max): {predicted_conditions['predicted_wind_speed']:.2f} km/h")
    
    if predicted_et0 is not None:
        print(f"\nModel Predicted ET0: {predicted_et0:.4f} mm")
        if predicted_et0 < predicted_conditions['target_et0_threshold']:
            print(f"✓ Predicted ET0 is below target threshold of {predicted_conditions['target_et0_threshold']}mm")
        else:
            print(f"✗ Predicted ET0 exceeds target threshold of {predicted_conditions['target_et0_threshold']}mm")
    
    if 'historical_mean_et0' in predicted_conditions:
        print(f"\nHistorical Reference:")
        print(f"  - Mean ET0 from similar conditions: {predicted_conditions['historical_mean_et0']:.4f} mm")
        print(f"  - Sample size: {predicted_conditions['historical_sample_count']} records")


# Main execution for standalone use
if __name__ == "__main__":
    from spark_data_loader import (
        create_spark_session, 
        load_weather_data, 
        load_location_data,
        join_weather_location
    )
    
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define data paths
    weather_path = os.path.join(project_root, "dataset", "weatherData.csv")
    location_path = os.path.join(project_root, "dataset", "locationData.csv")
    
    print("=" * 60)
    print("Sri Lanka Weather Analytics - ML Dataset Preparation")
    print("=" * 60)
    
    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session("MLDatasetPreparation")
    
    # Load data
    print("\n2. Loading weather data...")
    weather_df = load_weather_data(spark, weather_path)
    
    print("\n3. Loading location data...")
    location_df = load_location_data(spark, location_path)
    
    # Join datasets
    print("\n4. Joining weather and location data...")
    joined_df = join_weather_location(weather_df, location_df)
    
    # Prepare ML dataset
    print("\n5. Preparing ML dataset for May evapotranspiration prediction...")
    ml_dataset = prepare_ml_dataset(joined_df, use_ml_imputer=False)
    
    # Get statistics
    print("\n6. Dataset Statistics:")
    stats = get_dataset_statistics(ml_dataset)
    print(f"   Total records: {stats['total_records']}")
    print(f"   Null counts after imputation: {stats['null_counts']}")
    
    # Validate dataset
    print("\n7. Validating ML dataset...")
    validation = validate_ml_dataset(ml_dataset)
    print(f"   Is valid: {validation['is_valid']}")
    if validation["issues"]:
        print("   Issues:")
        for issue in validation["issues"]:
            print(f"   - {issue}")
    
    # Show sample data
    print("\n8. Sample ML dataset:")
    ml_dataset.show(10, truncate=False)
    
    # Show feature statistics
    print("\n9. Feature Statistics:")
    ml_dataset.select(FEATURE_COLUMNS + [TARGET_COLUMN]).describe().show()
    
    # Perform train/validation split (Requirements 5.1)
    print("\n10. Splitting dataset into training and validation sets...")
    split_result = prepare_train_validation_datasets(ml_dataset, seed=DEFAULT_SEED)
    
    train_df = split_result["train_df"]
    validation_df = split_result["validation_df"]
    verification = split_result["verification"]
    split_stats = split_result["statistics"]
    
    print(f"    Total records: {split_stats['total_records']}")
    print(f"    Training records: {split_stats['training_records']}")
    print(f"    Validation records: {split_stats['validation_records']}")
    print(f"    Random seed: {split_stats['seed']}")
    print(f"\n    Split Verification:")
    print(f"    - Actual train ratio: {verification['actual_train_ratio']:.4f}")
    print(f"    - Actual validation ratio: {verification['actual_validation_ratio']:.4f}")
    print(f"    - Expected train ratio: {verification['expected_train_ratio']}")
    print(f"    - Deviation: {verification['deviation']:.4f}")
    print(f"    - Within tolerance (±{verification['tolerance']}): {verification['is_valid']}")
    print(f"    - {verification['message']}")
    
    # Show sample training data
    print("\n11. Sample Training Data:")
    train_df.show(5, truncate=False)
    
    # Show sample validation data
    print("\n12. Sample Validation Data:")
    validation_df.show(5, truncate=False)
    
    # Train and evaluate models (Requirements 5.3)
    print("\n" + "=" * 60)
    print("13. Training Regression Models")
    print("=" * 60)
    
    # Train Linear Regression model
    print("\n13.1 Training Linear Regression model...")
    lr_result = train_linear_regression(
        train_df,
        feature_cols=FEATURE_COLUMNS,
        target_col=TARGET_COLUMN,
        max_iter=DEFAULT_MAX_ITER,
        reg_param=DEFAULT_REG_PARAM
    )
    
    # Evaluate Linear Regression on validation set
    print("\n13.2 Evaluating Linear Regression on validation set...")
    lr_eval = evaluate_model(lr_result, validation_df)
    
    # Print Linear Regression summary
    print_model_summary(lr_result, lr_eval)
    
    # Train Random Forest Regressor model
    print("\n13.3 Training Random Forest Regressor model...")
    rf_result = train_random_forest_regressor(
        train_df,
        feature_cols=FEATURE_COLUMNS,
        target_col=TARGET_COLUMN,
        num_trees=DEFAULT_NUM_TREES,
        max_depth=DEFAULT_MAX_DEPTH
    )
    
    # Evaluate Random Forest on validation set
    print("\n13.4 Evaluating Random Forest Regressor on validation set...")
    rf_eval = evaluate_model(rf_result, validation_df)
    
    # Print Random Forest summary
    print_model_summary(rf_result, rf_eval)
    
    # Compare models
    print("\n" + "=" * 60)
    print("14. Model Comparison (Requirements 5.4)")
    print("=" * 60)
    print(f"\n{'Model':<25} {'RMSE':<12} {'R²':<12} {'MAE':<12}")
    print("-" * 60)
    print(f"{'Linear Regression':<25} {lr_eval['rmse']:<12.4f} {lr_eval['r2']:<12.4f} {lr_eval['mae']:<12.4f}")
    print(f"{'Random Forest':<25} {rf_eval['rmse']:<12.4f} {rf_eval['r2']:<12.4f} {rf_eval['mae']:<12.4f}")
    
    best_model = "Linear Regression" if lr_eval['rmse'] < rf_eval['rmse'] else "Random Forest"
    best_model_result = lr_result if lr_eval['rmse'] < rf_eval['rmse'] else rf_result
    best_model_eval = lr_eval if lr_eval['rmse'] < rf_eval['rmse'] else rf_eval
    print(f"\nBest model based on RMSE: {best_model}")
    
    # Show sample predictions
    print("\n15. Sample Predictions (Linear Regression):")
    lr_eval['predictions_df'].select(
        "location_id", "year", 
        *FEATURE_COLUMNS, 
        TARGET_COLUMN, 
        "prediction"
    ).show(10, truncate=False)
    
    # Predict conditions for May 2026 with ET0 < 1.5mm (Requirements 5.5)
    print("\n" + "=" * 60)
    print("16. May 2026 Prediction (Requirements 5.5)")
    print("=" * 60)
    
    # Use the best model for prediction
    print(f"\nUsing {best_model} model for May 2026 prediction...")
    
    # Predict conditions that would result in ET0 < 1.5mm
    predicted_conditions = predict_conditions_for_low_et0(
        best_model_result,
        train_df,
        target_et0=TARGET_ET0_THRESHOLD
    )
    
    # Generate prediction DataFrame for May 2026
    may_2026_df = generate_may_2026_prediction(
        spark,
        best_model_result,
        predicted_conditions
    )
    
    # Get the predicted ET0 value
    predicted_et0 = may_2026_df.select("prediction").collect()[0][0]
    
    # Print the prediction summary
    print_may_2026_prediction(predicted_conditions, predicted_et0)
    
    # Show the full prediction DataFrame
    print("\nMay 2026 Prediction DataFrame:")
    may_2026_df.select(
        "year", "month",
        "precipitation_hours", "sunshine_duration", "wind_speed_10m_max",
        "prediction"
    ).show(truncate=False)
    
    print("\n" + "=" * 60)
    print("ML Model Training and Prediction Complete!")
    print("=" * 60)
    print("\nSummary:")
    print(f"  - Best Model: {best_model}")
    print(f"  - Validation RMSE: {best_model_eval['rmse']:.4f}")
    print(f"  - Validation R²: {best_model_eval['r2']:.4f}")
    print(f"  - Predicted conditions for May 2026 with ET0 < 1.5mm:")
    print(f"    * Precipitation Hours: {predicted_conditions['predicted_precipitation_hours']:.2f}")
    print(f"    * Sunshine Duration: {predicted_conditions['predicted_sunshine_duration']:.2f}")
    print(f"    * Wind Speed: {predicted_conditions['predicted_wind_speed']:.2f}")
    print(f"  - Model Predicted ET0: {predicted_et0:.4f} mm")
    
    # Stop Spark session
    spark.stop()
