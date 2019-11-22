package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal4_9Calculator
 *
 * @author Chilseasai@
 */
public class Zonal4_9Calculator {
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
        return 2.99;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 4.93;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 5.63;
        } else { // shippingWeight > 2.0
            return 5.63 + 0.29 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 5.63;
        } else {
            return 5.63 + 0.29 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 11.05;
        } else {
            return 11.05 + 0.35 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 33.06;
        } else {
            return 33.06 + 0.4 * (shippingWeight - 90.0);
        }
    }
}
