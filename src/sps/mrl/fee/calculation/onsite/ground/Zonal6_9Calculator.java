package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal6_9Calculator
 *
 * @author Chilseasai@
 */
public class Zonal6_9Calculator {
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
        return 3.34;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 6.04;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 6.96;
        } else { // shippingWeight > 2.0
            return 6.96 + 0.42 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 6.96;
        } else {
            return 6.96 + 0.42 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 12.39;
        } else {
            return 12.39 + 0.42 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 34.4;
        } else {
            return 34.4 + 0.43 * (shippingWeight - 90.0);
        }
    }
}
