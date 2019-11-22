package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal7_7Calculator
 *
 * @author Chilseasai@
 */
public class Zonal7_7Calculator {
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
            return 6.16;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 7.13;
        } else { // shippingWeight > 2.0
            return 7.13 + 0.49 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 7.13;
        } else {
            return 7.13 + 0.49 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 12.55;
        } else {
            return 12.55 + 0.47 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 34.56;
        } else {
            return 34.56 + 0.45 * (shippingWeight - 90.0);
        }
    }
}
