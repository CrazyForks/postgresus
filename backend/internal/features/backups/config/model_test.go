package backups_config

import (
	"testing"

	"databasus-backend/internal/features/intervals"
	plans "databasus-backend/internal/features/plan"
	"databasus-backend/internal/util/period"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Validate_WhenStoragePeriodIsWeekAndPlanAllowsMonth_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodWeek

	plan := createUnlimitedPlan()
	plan.MaxStoragePeriod = period.PeriodMonth

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenStoragePeriodIsYearAndPlanAllowsMonth_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodYear

	plan := createUnlimitedPlan()
	plan.MaxStoragePeriod = period.PeriodMonth

	err := config.Validate(plan)
	assert.EqualError(t, err, "storage period exceeds plan limit")
}

func Test_Validate_WhenStoragePeriodIsForeverAndPlanAllowsForever_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodForever

	plan := createUnlimitedPlan()
	plan.MaxStoragePeriod = period.PeriodForever

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenStoragePeriodIsForeverAndPlanAllowsYear_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodForever

	plan := createUnlimitedPlan()
	plan.MaxStoragePeriod = period.PeriodYear

	err := config.Validate(plan)
	assert.EqualError(t, err, "storage period exceeds plan limit")
}

func Test_Validate_WhenStoragePeriodEqualsExactPlanLimit_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodMonth

	plan := createUnlimitedPlan()
	plan.MaxStoragePeriod = period.PeriodMonth

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenBackupSize100MBAndPlanAllows500MB_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupSizeMB = 100

	plan := createUnlimitedPlan()
	plan.MaxBackupSizeMB = 500

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenBackupSize500MBAndPlanAllows100MB_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupSizeMB = 500

	plan := createUnlimitedPlan()
	plan.MaxBackupSizeMB = 100

	err := config.Validate(plan)
	assert.EqualError(t, err, "max backup size exceeds plan limit")
}

func Test_Validate_WhenBackupSizeIsUnlimitedAndPlanAllowsUnlimited_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupSizeMB = 0

	plan := createUnlimitedPlan()
	plan.MaxBackupSizeMB = 0

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenBackupSizeIsUnlimitedAndPlanHas500MBLimit_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupSizeMB = 0

	plan := createUnlimitedPlan()
	plan.MaxBackupSizeMB = 500

	err := config.Validate(plan)
	assert.EqualError(t, err, "max backup size exceeds plan limit")
}

func Test_Validate_WhenBackupSizeEqualsExactPlanLimit_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupSizeMB = 500

	plan := createUnlimitedPlan()
	plan.MaxBackupSizeMB = 500

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenTotalSize1GBAndPlanAllows5GB_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupsTotalSizeMB = 1000

	plan := createUnlimitedPlan()
	plan.MaxBackupsTotalSizeMB = 5000

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenTotalSize5GBAndPlanAllows1GB_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupsTotalSizeMB = 5000

	plan := createUnlimitedPlan()
	plan.MaxBackupsTotalSizeMB = 1000

	err := config.Validate(plan)
	assert.EqualError(t, err, "max total backups size exceeds plan limit")
}

func Test_Validate_WhenTotalSizeIsUnlimitedAndPlanAllowsUnlimited_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupsTotalSizeMB = 0

	plan := createUnlimitedPlan()
	plan.MaxBackupsTotalSizeMB = 0

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenTotalSizeIsUnlimitedAndPlanHas1GBLimit_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupsTotalSizeMB = 0

	plan := createUnlimitedPlan()
	plan.MaxBackupsTotalSizeMB = 1000

	err := config.Validate(plan)
	assert.EqualError(t, err, "max total backups size exceeds plan limit")
}

func Test_Validate_WhenTotalSizeEqualsExactPlanLimit_ValidationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupsTotalSizeMB = 5000

	plan := createUnlimitedPlan()
	plan.MaxBackupsTotalSizeMB = 5000

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenAllLimitsAreUnlimitedInPlan_AnyConfigurationPasses(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodForever
	config.MaxBackupSizeMB = 0
	config.MaxBackupsTotalSizeMB = 0

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.NoError(t, err)
}

func Test_Validate_WhenMultipleLimitsExceeded_ValidationFailsWithFirstError(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodYear
	config.MaxBackupSizeMB = 500
	config.MaxBackupsTotalSizeMB = 5000

	plan := createUnlimitedPlan()
	plan.MaxStoragePeriod = period.PeriodMonth
	plan.MaxBackupSizeMB = 100
	plan.MaxBackupsTotalSizeMB = 1000

	err := config.Validate(plan)
	assert.Error(t, err)
	assert.EqualError(t, err, "storage period exceeds plan limit")
}

func Test_Validate_WhenConfigHasInvalidIntervalButPlanIsValid_ValidationFailsOnInterval(
	t *testing.T,
) {
	config := createValidBackupConfig()
	config.BackupIntervalID = uuid.Nil
	config.BackupInterval = nil

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "backup interval is required")
}

func Test_Validate_WhenIntervalIsMissing_ValidationFailsRegardlessOfPlan(t *testing.T) {
	config := createValidBackupConfig()
	config.BackupIntervalID = uuid.Nil
	config.BackupInterval = nil

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "backup interval is required")
}

func Test_Validate_WhenRetryEnabledButMaxTriesIsZero_ValidationFailsRegardlessOfPlan(t *testing.T) {
	config := createValidBackupConfig()
	config.IsRetryIfFailed = true
	config.MaxFailedTriesCount = 0

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "max failed tries count must be greater than 0")
}

func Test_Validate_WhenEncryptionIsInvalid_ValidationFailsRegardlessOfPlan(t *testing.T) {
	config := createValidBackupConfig()
	config.Encryption = "INVALID"

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "encryption must be NONE or ENCRYPTED")
}

func Test_Validate_WhenStoragePeriodIsEmpty_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = ""

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "store period is required")
}

func Test_Validate_WhenMaxBackupSizeIsNegative_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupSizeMB = -100

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "max backup size must be non-negative")
}

func Test_Validate_WhenMaxTotalSizeIsNegative_ValidationFails(t *testing.T) {
	config := createValidBackupConfig()
	config.MaxBackupsTotalSizeMB = -1000

	plan := createUnlimitedPlan()

	err := config.Validate(plan)
	assert.EqualError(t, err, "max backups total size must be non-negative")
}

func Test_Validate_WhenPlanIsNil_OnlyBasicValidationsApply(t *testing.T) {
	config := createValidBackupConfig()
	config.StorePeriod = period.PeriodForever
	config.MaxBackupSizeMB = 0
	config.MaxBackupsTotalSizeMB = 0

	err := config.Validate(nil)
	assert.NoError(t, err)
}

func Test_Validate_WhenPlanLimitsAreAtBoundary_ValidationWorks(t *testing.T) {
	tests := []struct {
		name          string
		configPeriod  period.Period
		planPeriod    period.Period
		configSize    int64
		planSize      int64
		configTotal   int64
		planTotal     int64
		shouldSucceed bool
	}{
		{
			name:          "all values just under limit",
			configPeriod:  period.PeriodWeek,
			planPeriod:    period.PeriodMonth,
			configSize:    99,
			planSize:      100,
			configTotal:   999,
			planTotal:     1000,
			shouldSucceed: true,
		},
		{
			name:          "all values equal to limit",
			configPeriod:  period.PeriodMonth,
			planPeriod:    period.PeriodMonth,
			configSize:    100,
			planSize:      100,
			configTotal:   1000,
			planTotal:     1000,
			shouldSucceed: true,
		},
		{
			name:          "period just over limit",
			configPeriod:  period.Period3Month,
			planPeriod:    period.PeriodMonth,
			configSize:    100,
			planSize:      100,
			configTotal:   1000,
			planTotal:     1000,
			shouldSucceed: false,
		},
		{
			name:          "size just over limit",
			configPeriod:  period.PeriodMonth,
			planPeriod:    period.PeriodMonth,
			configSize:    101,
			planSize:      100,
			configTotal:   1000,
			planTotal:     1000,
			shouldSucceed: false,
		},
		{
			name:          "total size just over limit",
			configPeriod:  period.PeriodMonth,
			planPeriod:    period.PeriodMonth,
			configSize:    100,
			planSize:      100,
			configTotal:   1001,
			planTotal:     1000,
			shouldSucceed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidBackupConfig()
			config.StorePeriod = tt.configPeriod
			config.MaxBackupSizeMB = tt.configSize
			config.MaxBackupsTotalSizeMB = tt.configTotal

			plan := createUnlimitedPlan()
			plan.MaxStoragePeriod = tt.planPeriod
			plan.MaxBackupSizeMB = tt.planSize
			plan.MaxBackupsTotalSizeMB = tt.planTotal

			err := config.Validate(plan)
			if tt.shouldSucceed {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func createValidBackupConfig() *BackupConfig {
	intervalID := uuid.New()
	return &BackupConfig{
		DatabaseID:            uuid.New(),
		IsBackupsEnabled:      true,
		StorePeriod:           period.PeriodMonth,
		BackupIntervalID:      intervalID,
		BackupInterval:        &intervals.Interval{ID: intervalID},
		SendNotificationsOn:   []BackupNotificationType{},
		IsRetryIfFailed:       false,
		MaxFailedTriesCount:   3,
		Encryption:            BackupEncryptionNone,
		MaxBackupSizeMB:       100,
		MaxBackupsTotalSizeMB: 1000,
	}
}

func createUnlimitedPlan() *plans.DatabasePlan {
	return &plans.DatabasePlan{
		DatabaseID:            uuid.New(),
		MaxBackupSizeMB:       0,
		MaxBackupsTotalSizeMB: 0,
		MaxStoragePeriod:      period.PeriodForever,
	}
}
