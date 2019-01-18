
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		AverageRating averageRating= new AverageRating();
		AverageRatingGenerator averageRatingGenerator= new AverageRatingGenerator();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		TopK topk =new TopK();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String averageRatingList= args[6];
		String averageRatingMatrix=args[7];
		String num_movies= args[8];
		//We create path1, path2, instead of pass args to every object to exhibite the complete inputs and outputs of each object.
		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {userMovieListOutputDir, averageRatingList};
		String[] path5 = {averageRatingList, normalizeDir, averageRatingMatrix};
		String[] path6 = {normalizeDir, rawInput, averageRatingMatrix, multiplicationDir};
		String[] path7 = {multiplicationDir, sumDir};
		String[] path8 = {rawInput, sumDir, num_movies};

		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		averageRating.main(path4);
		averageRatingGenerator.main(path5);
		multiplication.main(path6);
		sum.main(path7);
		topk.main(path8);

	}

}
