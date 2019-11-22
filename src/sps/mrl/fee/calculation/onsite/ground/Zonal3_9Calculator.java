package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal3_9Calculator
 *
 * @author Chilseasai@
 */
public class Zonal3_9Calculator {
    public double getAmount(final TestCaseItem item) {
        switch (item.getSizeTier()) {
            case "UsSmallStandardSize":
                return calculateSmallStandardSize(item.getShippingWeight());
            case "UsLargeStandardSize":
                return calculateLargeStandardSize(item.getShippingWeight());
            case "UsSmallOversize":
                return calculateSmallOversize(item.getShippingWeight());
            case "UsMediumOversize":
                return calculateMediumOversize(item.getShippingWeight());
            case "UsLargeOversize":
                return calculateLargeOversize(item.getShippingWeight());
            default:
                return 0.0;
        }
    }

    private double calculateSmallStandardSize(final double shippingWeight) {
        return 2.81;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 4.38;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 4.96;
        } else { // shippingWeight > 2.0
            return 4.96 + 0.23 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 4.96;
        } else {
            return 4.96 + 0.23 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 10.38;
        } else {
            return 10.38 + 0.31 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 32.39;
        } else {
            return 32.39 + 0.38 * (shippingWeight - 90.0);
        }
    }
}
