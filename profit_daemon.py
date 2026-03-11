"""
Profit-Triggered Reinvestment Daemon
Core Module of Recursive Self-Improvement Engine
Monitors wallet inflows and automatically allocates percentages to reserve buckets
"""

import logging
import time
import json
import os
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
import firebase_admin
from firebase_admin import credentials, firestore
from enum import Enum


class AllocationBucket(str, Enum):
    """Predefined reserve buckets for profit allocation"""
    API_CREDITS = "api_credits"
    HARDWARE_FUND = "hardware_fund"
    TRADING_CAPITAL = "trading_capital"
    EMERGENCY_RESERVE = "emergency_reserve"
    DEVELOPMENT_FUND = "development_fund"


class TransactionType(str, Enum):
    """Types of wallet transactions"""
    INFLOW = "inflow"
    OUTFLOW = "outflow"
    INTERNAL = "internal"


@dataclass
class Transaction:
    """Represents a wallet transaction with validation metadata"""
    transaction_id: str
    amount: Decimal
    currency: str
    timestamp: datetime
    transaction_type: TransactionType
    source: str
    description: str
    is_verified: bool = False
    verification_timestamp: Optional[datetime] = None
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))


@dataclass
class AllocationRule:
    """Defines allocation rules for profit distribution"""
    bucket: AllocationBucket
    percentage: Decimal  # 0-1 scale
    priority: int  # 1=highest priority
    minimum_threshold: Decimal = Decimal('0')
    maximum_amount: Optional[Decimal] = None

    def __post_init__(self):
        if not isinstance(self.percentage, Decimal):
            self.percentage = Decimal(str(self.percentage))
        if not isinstance(self.minimum_threshold, Decimal):
            self.minimum_threshold = Decimal(str(self.minimum_threshold))
        if self.maximum_amount and not isinstance(self.maximum_amount, Decimal):
            self.maximum_amount = Decimal(str(self.maximum_amount))


@dataclass
class AllocationResult:
    """Result of profit allocation calculation"""
    bucket: AllocationBucket
    allocated_amount: Decimal
    original_percentage: Decimal
    actual_percentage: Decimal
    capped: bool = False
    threshold_triggered: bool = False


class ProfitReinvestmentDaemon:
    """
    Autonomous daemon that monitors wallet inflows and automatically allocates
    profits to predefined reserve buckets according to configured rules.
    
    Architecture Choices:
    1. Decimal for financial calculations - avoids float rounding errors
    2. Dataclasses for immutable data structures - ensures type safety
    3. Firebase Firestore for state persistence - enables distributed operation
    4. Event-driven with idempotency - prevents duplicate processing
    5. Configurable allocation rules - allows runtime adjustments
    """
    
    def __init__(self, config_path: str = "config/reinvestment_config.json"):
        """
        Initialize the daemon with configuration
        
        Args:
            config_path: Path to configuration file
        """
        # Initialize logging
        self.logger = self._setup_logging()
        self.logger.info("Initializing Profit Reinvestment Daemon")
        
        # Load configuration
        self.config = self._load_configuration(config_path)
        
        # Initialize Firebase
        self.db = self._initialize_firebase()
        
        # Initialize allocation rules
        self.allocation_rules = self._initialize_allocation_rules()
        
        # State tracking
        self.processed_transactions = set()
        self.last_check_time = datetime.utcnow()
        
        self.logger.info(f"Daemon initialized with {len(self.allocation_rules)} allocation rules")
    
    def _setup_logging(self) -> logging.Logger:
        """Configure structured logging for the daemon"""
        logger = logging.getLogger("ProfitReinvestmentDaemon")
        logger.setLevel(logging.INFO)
        
        # Console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _load_configuration(self, config_path: str) -> Dict:
        """
        Load configuration from JSON file with validation
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
            KeyError: If required configuration keys are missing
        """
        if not os.path.exists(config_path):
            self.logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in config file: {e}")
            raise
        
        # Validate required configuration sections
        required_sections = ['firebase', 'monitoring', 'allocation']
        for section in required_sections:
            if section not in config:
                self.logger.error(f"Missing required config section: {section}")
                raise KeyError(f"Missing required config section: {section}")
        
        self.logger.info(f"Configuration loaded from {config_path}")
        return config
    
    def _initialize_firebase(self) -> firestore.Client:
        """
        Initialize Firebase connection with error handling
        
        Returns:
            Firestore client instance
            
        Raises:
            ValueError: If Firebase credentials are invalid
        """
        try:
            # Check for Firebase credentials in config
            firebase_config = self.config.get('firebase', {})
            
            if 'credential_path' in firebase_config:
                # Use service account file
                cred_path = firebase_config['credential_path']
                if not os.path.exists(cred_path):
                    self.logger.error(f"Firebase credentials not found: {cred_path}")
                    raise FileNotFoundError(f"Firebase credentials not found: {cred_path}")
                
                cred = credentials.Certificate(cred_path)
            else:
                # Use application default credentials
                cred = credentials.ApplicationDefault()
            
            # Initialize Firebase app
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            
            # Get Firestore client
            db = firestore.client()
            
            # Test connection
            test_ref = db.collection('system_health').document('connection_test')
            test_ref.set({
                'timestamp': firestore.SERVER_TIMESTAMP,
                'status': 'connected'
            })
            
            self.logger.info("Firebase Firestore initialized successfully")
            return db
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Firebase: {e}")
            raise ValueError(f"Firebase initialization failed: {e}")
    
    def _initialize_allocation_rules(self) -> List[AllocationRule]:
        """
        Initialize allocation rules from configuration
        
        Returns:
            List of AllocationRule objects
        """
        rules_config = self.config.get('allocation', {}).get('rules', [])
        rules = []
        
        for rule_config in rules_config:
            try:
                rule = AllocationRule(
                    bucket=AllocationBucket(rule_config['bucket']),
                    percentage=Decimal(str(rule_config['percentage'])),
                    priority=int(rule_config.get('priority', 99)),
                    minimum_threshold=Decimal(str(rule_config.get('minimum_threshold', '0'))),
                    maximum_amount=Decimal(str(rule_config['maximum_amount'])) 
                        if 'maximum_amount' in rule_config else None
                )
                rules.append(rule)
                self.logger.debug(f"Initialized allocation rule: {rule.bucket} = {rule.percentage*100}%")
            except (KeyError, ValueError) as e:
                self.logger.error(f"Invalid allocation rule configuration: {rule_config} - {e}")
                continue
        
        # Sort rules by priority (ascending - lower number = higher priority)
        rules.sort(key=lambda x: x.priority)
        
        if not rules:
            self.logger.warning("No allocation rules configured. Using defaults.")
            rules = self._get_default_allocation_rules()
        
        # Validate percentages sum to <= 1
        total_percentage = sum(rule.percentage for rule in rules)
        if total_percentage > Decimal('1'):
            self.logger.warning(f"Allocation percentages sum to {total_percentage*100}% > 100%. Adjusting...")
        
        return rules
    
    def _get_default_allocation_rules(self) -> List[AllocationRule]:
        """Provide sensible default allocation rules"""
        return [
            AllocationRule(
                bucket=AllocationBucket.EMERGENCY_RESERVE,
                percentage=Decimal('0.10'),
                priority=1,
                minimum_threshold=Decimal('100')
            ),
            AllocationRule(
                bucket=AllocationBucket.API_CREDITS,
                percentage=Decimal('0.30'),
                priority=2,
                minimum_threshold=Decimal('50')
            ),
            AllocationRule(
                bucket=AllocationBucket.HARDWARE_FUND,
                percentage=Decimal('0.25'),
                priority=3
            ),
            AllocationRule(
                bucket=AllocationBucket.TRADING_CAPITAL,
                percentage=Decimal('0.25'),
                priority=4
            ),
            AllocationRule(
                bucket=AllocationBucket.DEVELOPMENT_FUND,
                percentage=Decimal('0.10'),
                priority=5
            )
        ]
    
    def _get_wallet_transactions(self, since: datetime) -> List[Transaction]:
        """
        Retrieve wallet transactions since specified time
        
        Args:
            since: Timestamp to filter transactions
            
        Returns:
            List of Transaction objects
            
        Note: This is a mock implementation. In production, integrate with
        actual wallet/exchange APIs (e.g., ccxt, blockchain explorers)
        """
        # Mock implementation - replace with actual API integration
        transactions = []
        
        try:
            # Example: Query Firebase for new transactions
            # In reality, you'd query your transaction database or external APIs
            transactions_ref = self.db.collection('wallet_transactions')
            query = transactions_ref.where('timestamp', '>=', since) \
                                   .where('processed', '==', False) \
                                   .where('type', '==', 'inflow') \
                                   .limit(100)
            
            docs = query.stream()
            
            for doc in docs:
                data = doc.to_dict()
                try:
                    transaction = Transaction(
                        transaction_id=data.get('id', doc.id),
                        amount=Decimal(str(data['amount'])),
                        currency=data.get('currency', 'USD'),
                        timestamp=data['timestamp'],
                        transaction_type=TransactionType(data.get('type', 'inflow')),
                        source=data.get('source', 'unknown'),
                        description=data.get('description', ''),
                        is_verified=data.get('verified', False),
                        tags=data.get('tags', [])
                    )
                    
                    # Additional validation
                    if transaction.amount > Decimal('0') and transaction.is_verified:
                        transactions.append(transaction)
                        
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Invalid transaction data {doc.id}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error fetching transactions: {e}")
            # Fallback to mock data for development
            if os.getenv('ENVIRONMENT') == 'development':
                transactions = self._get_mock_transactions(since)
        
        return transactions
    
    def _get_mock_transactions(self, since: datetime) -> List[Transaction]:
        """Generate mock transactions for development/testing"""
        # This is for development only
        return [
            Transaction(
                transaction_id="mock_tx_001",
                amount=Decimal('500.00'),