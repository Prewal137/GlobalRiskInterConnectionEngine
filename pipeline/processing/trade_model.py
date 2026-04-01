"""
Trade Shock Prediction Model
Trains an XGBoost classifier to predict trade shocks from trade features.
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from xgboost import XGBClassifier
import joblib
import os


def load_data(filepath: str) -> pd.DataFrame:
    """Load trade feature data from CSV file."""
    print(f"Loading data from {filepath}...")
    df = pd.read_csv(filepath)
    print(f"✓ Loaded {len(df)} rows with {len(df.columns)} columns")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the dataset by handling missing values and ensuring correct data types."""
    print("\nCleaning data...")
    
    # Drop rows with NaN values
    initial_rows = len(df)
    df = df.dropna()
    dropped_rows = initial_rows - len(df)
    if dropped_rows > 0:
        print(f"  - Dropped {dropped_rows} rows with NaN values")
    
    # Ensure Shock column is integer
    df['Shock'] = df['Shock'].astype(int)
    print("  - Converted Shock column to integer")
    
    print(f"✓ Cleaned dataset: {len(df)} rows")
    return df


def define_features(df: pd.DataFrame):
    """Define feature matrix X and target vector y."""
    print("\nDefining features and target...")
    
    # Columns to drop (non-feature columns and data leakage columns)
    drop_columns = ['Country', 'Shock', 'Growth', 'Export_Growth', 'Import_Growth']
    
    # Define features
    feature_columns = [col for col in df.columns if col not in drop_columns]
    
    print(f"  - Features ({len(feature_columns)}): {feature_columns}")
    print(f"  - Target: Shock")
    print(f"  - Removed data leakage columns: Growth, Export_Growth, Import_Growth")
    
    X = df[feature_columns]
    y = df['Shock']
    
    return X, y, feature_columns


def split_data(X: pd.DataFrame, y: pd.Series):
    """Split data into training and test sets with stratification."""
    print("\nSplitting data into train/test sets...")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, 
        y, 
        test_size=0.2, 
        random_state=42, 
        stratify=y
    )
    
    print(f"  - Training set: {len(X_train)} samples ({len(X_train)/len(X)*100:.1f}%)")
    print(f"  - Test set: {len(X_test)} samples ({len(X_test)/len(X)*100:.1f}%)")
    
    return X_train, X_test, y_train, y_test


def train_model(X_train: pd.DataFrame, y_train: pd.Series):
    """Train XGBoost classifier model."""
    print("\nTraining XGBoost model...")
    
    model = XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)
    print("✓ Model training completed")
    
    return model


def evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series):
    """Evaluate model performance on test set."""
    print("\nEvaluating model performance...")
    
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='weighted')
    recall = recall_score(y_test, y_pred, average='weighted')
    f1 = f1_score(y_test, y_pred, average='weighted')
    conf_matrix = confusion_matrix(y_test, y_pred)
    
    # Print metrics
    print(f"\n{'='*50}")
    print("MODEL PERFORMANCE METRICS")
    print(f"{'='*50}")
    print(f"Accuracy:  {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"Precision: {precision:.4f} ({precision*100:.2f}%)")
    print(f"Recall:    {recall:.4f} ({recall*100:.2f}%)")
    print(f"F1 Score:  {f1:.4f} ({f1*100:.2f}%)")
    print(f"\nConfusion Matrix:")
    print(conf_matrix)
    print(f"{'='*50}")
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'confusion_matrix': conf_matrix
    }


def get_feature_importance(model, feature_columns: list):
    """Extract and display feature importance scores."""
    print("\nFEATURE IMPORTANCE SCORES")
    print(f"{'='*50}")
    
    importance_df = pd.DataFrame({
        'Feature': feature_columns,
        'Importance': model.feature_importances_
    })
    
    # Sort by importance
    importance_df = importance_df.sort_values('Importance', ascending=False)
    
    for _, row in importance_df.iterrows():
        print(f"{row['Feature']:25s}: {row['Importance']:.6f}")
    
    print(f"{'='*50}")
    
    return importance_df


def save_model(model, filepath: str):
    """Save trained model using joblib."""
    print(f"\nSaving model to {filepath}...")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    joblib.dump(model, filepath)
    print(f"✓ Model saved successfully")


def print_summary(df: pd.DataFrame, X_train: pd.DataFrame, X_test: pd.DataFrame):
    """Print dataset summary statistics."""
    print("\n" + "="*50)
    print("DATASET SUMMARY")
    print("="*50)
    print(f"Dataset Shape: {df.shape} (rows: {df.shape[0]}, columns: {df.shape[1]})")
    print(f"Training Set Size: {len(X_train)} samples")
    print(f"Test Set Size: {len(X_test)} samples")
    
    if 'Shock' in df.columns:
        num_shocks = (df['Shock'] == 1).sum()
        num_no_shocks = (df['Shock'] == 0).sum()
        print(f"Number of Shocks (1): {num_shocks}")
        print(f"Number of No Shocks (0): {num_no_shocks}")
        print(f"Shock Ratio: {num_shocks/len(df)*100:.2f}%")
    
    print("="*50)


def main():
    """Main pipeline for training trade shock prediction model."""
    print("="*50)
    print("TRADE SHOCK PREDICTION MODEL TRAINING")
    print("="*50)
    
    # File paths
    input_file = "data/processed/trade/trade_features_clean.csv"
    output_model = "models/trained/trade_model.pkl"
    
    # Step 1: Load data
    df = load_data(input_file)
    
    # Step 2: Clean data
    df = clean_data(df)
    
    # Step 3: Define features
    X, y, feature_columns = define_features(df)
    
    # Step 4: Split data
    X_train, X_test, y_train, y_test = split_data(X, y)
    
    # Step 5 & 6: Train model
    model = train_model(X_train, y_train)
    
    # Step 7: Evaluate model
    metrics = evaluate_model(model, X_test, y_test)
    
    # Step 8: Feature importance
    importance_df = get_feature_importance(model, feature_columns)
    
    # Step 9: Save model
    save_model(model, output_model)
    
    # Step 10: Print summary
    print_summary(df, X_train, X_test)
    
    print("\n✅ MODEL TRAINING COMPLETED SUCCESSFULLY!")
    print(f"📦 Model saved to: {output_model}")


if __name__ == "__main__":
    main()
