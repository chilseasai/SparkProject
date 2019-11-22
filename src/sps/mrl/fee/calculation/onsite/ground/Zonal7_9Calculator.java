package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal7_9Calculator
 *
 * @author Chilseasai@
 */
public class Zonal7_9Calculator {
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
        return 3.35;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 6.18;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 7.15;
        } else { // shippingWeight > 2.0
            return 7.15 + 0.51 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 7.15;
        } else {
            return 7.15 + 0.51 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 12.58;
        } else {
            return 12.58 + 0.48 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 34.59;
        } else {
            return 34.59 + 0.45 * (shippingWeight - 90.0);
        }
    }
}
