#' Fahrenheit conversion
#'
#' Convert degrees Fahrenheit temperatures to degrees Celsius
#' @param F_temp The temperature in degrees Fahrenheit
#' @return The temperature in degrees Celsius
#' @examples 
#' temp1 <- F_to_C(50);
#' temp2 <- F_to_C( c(50, 63, 23) );
#' @import dplyr
#' @import arrow
#' @export
read_modkit_bed_to_arrow <- function(file_path) {
  # Don't care too much about speed of writing, 
  # it's the speed of loading and subseting the result files that is important.
  # Read a modkit bedMethyl file as data_frame with read_tsv_arrow
  # Not sure why (faster - multicore read?) (instead of fread), but this works...
  # Need to add identifier and that does not work if not using as_data_frame = TRUE (?)
  # Remove duplicated columns (fraction_modified is still nice to keep, although it can be inferred?)
  read_tsv_arrow(file_path,
                 col_names = c("chr", 
                               "start", 
                               "end", 
                               "mod_base", 
                               "score", 
                               "strand", 
                               "start_2", 
                               "end_2", 
                               "color", 
                               "n_valid_cov", 
                               "fraction_modified", 
                               "n_mod", 
                               "n_canonical", 
                               "n_other_mod", 
                               "n_delete",
                               "n_fail",
                               "n_diff",
                               "n_nocall"),
                 col_select = c("chr", 
                                "start", 
                                "end", 
                                "mod_base", 
                                "strand", 
                                "n_valid_cov", 
                                "fraction_modified", 
                                "n_mod", 
                                "n_canonical", 
                                "n_other_mod", 
                                "n_delete",
                                "n_fail",
                                "n_diff",
                                "n_nocall"),
                 as_data_frame = TRUE)
}

convert_modkit_bed_to_parquet_hive <- function(file_path, output_dir) {
  # Use filname as identifier
  filename <- gsub(".*/", "", file_path)
  # How should it be partitioned? By name and chr? 
  write_dataset(dataset = read_modkit_bed_to_arrow(file_path) %>% 
                  mutate(file = filename),
                path = output_dir,
                format = "parquet",
                partitioning = c("file", "chr")
  )
}
