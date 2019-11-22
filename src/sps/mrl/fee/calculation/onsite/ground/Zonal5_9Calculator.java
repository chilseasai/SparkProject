package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal5_9Calculator
 *
 * @author Chilseasai@
 */
public class Zonal5_9Calculator {
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
        return 3.16;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 5.48;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 6.3;
        } else { // shippingWeight > 2.0
            return 6.3 + 0.36 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 6.3;
        } else {
            return 6.3 + 0.36 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 11.72;
        } else {
            return 11.72 + 0.39 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 33.73;
        } else {
            return 33.73 + 0.41 * (shippingWeight - 90.0);
        }
    }
}
