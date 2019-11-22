package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal6_2Calculator
 *
 * @author Chilseasai@
 */
public class Zonal6_2Calculator {
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
        return 3.21;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 5.65;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 6.5;
        } else { // shippingWeight > 2.0
            return 6.5 + 0.37 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 6.5;
        } else {
            return 6.5 + 0.37 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 11.92;
        } else {
            return 11.92 + 0.4 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 33.93;
        } else {
            return 33.93 + 0.42 * (shippingWeight - 90.0);
        }
    }
}
