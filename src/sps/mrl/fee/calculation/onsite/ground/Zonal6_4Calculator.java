package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal6_4Calculator
 *
 * @author Chilseasai@
 */
public class Zonal6_4Calculator {
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
        return 3.25;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 5.76;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 6.63;
        } else { // shippingWeight > 2.0
            return 6.63 + 0.39 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 6.63;
        } else {
            return 6.63 + 0.39 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 12.05;
        } else {
            return 12.05 + 0.41 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 34.07;
        } else {
            return 34.07 + 0.42 * (shippingWeight - 90.0);
        }
    }
}
